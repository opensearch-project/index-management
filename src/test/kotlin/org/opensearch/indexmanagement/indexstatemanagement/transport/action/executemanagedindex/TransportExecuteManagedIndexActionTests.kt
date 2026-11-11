/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.executemanagedindex

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.Version
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.PlainActionFuture
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.routing.IndexRoutingTable
import org.opensearch.cluster.routing.IndexShardRoutingTable
import org.opensearch.cluster.routing.OperationRouting
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.routing.ShardRoutingState
import org.opensearch.cluster.routing.TestShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.bytes.BytesArray
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.get.GetResult
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.DefaultIndexMetadataService
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.SkipExecution
import org.opensearch.indexmanagement.indexstatemanagement.randomManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.toJsonString
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.spi.indexstatemanagement.IndexMetadataService
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.tasks.Task
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportChannel
import org.opensearch.transport.TransportRequestHandler
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.node.NodeClient
import java.time.Instant

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class TransportExecuteManagedIndexActionTests : OpenSearchTestCase() {
    private val indexName = "my-index"
    private val indexUuid = "my-index-uuid"
    private val configIndex = Index(INDEX_MANAGEMENT_INDEX, "config-index-uuid")
    private val configShardId = ShardId(configIndex, 0)
    private lateinit var localNode: DiscoveryNode
    private lateinit var transportService: TransportService
    private lateinit var lockService: LockService

    @Before
    fun setup() {
        localNode = DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT)
        transportService = mock()
        lockService = mock()
        // the runner is a singleton: start every test from a cluster without mixed versions
        ManagedIndexRunner.registerSkipFlag(SkipExecution())
    }

    /**
     * Answers what the action asks of the client: the permission check (ManagedIndexRequest) succeeds unless [permissionFailure] is
     * set, and the get of the job document finds [jobDocument] or nothing, or fails with [getFailure].
     */
    private inner class FakeClient(
        private val permissionFailure: Exception? = null,
        private val jobDocument: String? = null,
        private val getFailure: Exception? = null,
    ) : NodeClient(Settings.EMPTY, mock<ThreadPool>()) {
        @Suppress("UNCHECKED_CAST")
        override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
            action: ActionType<Response>,
            request: Request,
            listener: ActionListener<Response>,
        ) {
            when (request) {
                is ManagedIndexRequest ->
                    if (permissionFailure != null) {
                        listener.onFailure(permissionFailure)
                    } else {
                        (listener as ActionListener<AcknowledgedResponse>).onResponse(AcknowledgedResponse(true))
                    }

                is GetRequest ->
                    if (getFailure != null) {
                        listener.onFailure(getFailure)
                    } else {
                        val source = jobDocument?.let { BytesArray(it) }
                        val result = GetResult(
                            request.index(), request.id(), SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, -1L,
                            source != null, source, null, null,
                        )
                        (listener as ActionListener<GetResponse>).onResponse(GetResponse(result))
                    }
            }
        }
    }

    private fun clusterState(
        withIndex: Boolean = true,
        nodes: List<DiscoveryNode> = listOf(localNode),
        configPrimary: Pair<DiscoveryNode, ShardRoutingState>? = localNode to ShardRoutingState.STARTED,
    ): ClusterState {
        val metadata = Metadata.builder().apply {
            if (withIndex) {
                put(
                    IndexMetadata.builder(indexName).settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid),
                    ).build(),
                    false,
                )
            }
        }.build()
        val routingTable = RoutingTable.builder().apply {
            configPrimary?.let { (node, state) ->
                add(
                    IndexRoutingTable.builder(configIndex).addIndexShard(
                        IndexShardRoutingTable.Builder(configShardId)
                            .addShard(TestShardRouting.newShardRouting(configShardId, node.id, true, state))
                            .build(),
                    ),
                )
            }
        }.build()
        val discoveryNodes = DiscoveryNodes.builder().apply {
            nodes.forEach { add(it) }
            localNodeId(localNode.id)
        }.build()
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).nodes(discoveryNodes).build()
    }

    private fun action(state: ClusterState, client: NodeClient = FakeClient()): TransportExecuteManagedIndexAction {
        val operationRouting: OperationRouting = mock()
        whenever(operationRouting.shardId(any(), any(), any(), any())).thenReturn(configShardId)
        val clusterService: ClusterService = mock()
        whenever(clusterService.state()).thenReturn(state)
        whenever(clusterService.localNode()).thenReturn(localNode)
        whenever(clusterService.operationRouting()).thenReturn(operationRouting)
        // IndexMetadataProvider registers a settings consumer when it is created
        whenever(clusterService.clusterSettings)
            .thenReturn(ClusterSettings(Settings.EMPTY, setOf<Setting<*>>(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN)))
        val services = mutableMapOf<String, IndexMetadataService>(DEFAULT_INDEX_TYPE to DefaultIndexMetadataService())
        return TransportExecuteManagedIndexAction(
            client, transportService, ActionFilters(emptySet()), clusterService, NamedXContentRegistry.EMPTY, lockService,
            IndexMetadataProvider(Settings.EMPTY, client, clusterService, services),
        )
    }

    private fun run(action: TransportExecuteManagedIndexAction): ISMStatusResponse {
        val future = PlainActionFuture.newFuture<ISMStatusResponse>()
        action.execute(mock<Task>(), ExecuteManagedIndexRequest(indexName), future)
        return future.actionGet(TIMEOUT_MILLIS)
    }

    private fun pauseExecution() {
        val olderNode = DiscoveryNode("old", buildNewFakeTransportAddress(), Version.V_2_0_0)
        val mixedVersionCluster: ClusterService = mock()
        whenever(mixedVersionCluster.state()).thenReturn(
            ClusterState.builder(ClusterState.EMPTY_STATE).nodes(DiscoveryNodes.builder().add(localNode).add(olderNode).build()).build(),
        )
        ManagedIndexRunner.registerSkipFlag(SkipExecution().also { it.sweepISMPluginVersion(mixedVersionCluster) })
    }

    private fun assertFailedWith(response: ISMStatusResponse, reasonPart: String) {
        assertEquals(0, response.updated)
        assertEquals(1, response.failedIndices.size)
        assertEquals(indexName, response.failedIndices[0].name)
        assertTrue("Unexpected reason [${response.failedIndices[0].reason}]", response.failedIndices[0].reason.contains(reasonPart))
    }

    fun `test a caller without permission gets a 403`() {
        val client = FakeClient(permissionFailure = OpenSearchSecurityException("denied", RestStatus.FORBIDDEN))

        val e = expectThrows(OpenSearchException::class.java) { run(action(clusterState(), client)) }

        assertEquals(RestStatus.FORBIDDEN, e.status())
    }

    fun `test a missing index from the permission check stays a plain index not found`() {
        val client = FakeClient(permissionFailure = IndexNotFoundException(indexName))

        expectThrows(IndexNotFoundException::class.java) { run(action(clusterState(), client)) }
    }

    fun `test a failure without a message is answered and does not hang`() {
        val client = FakeClient(permissionFailure = IllegalStateException())

        expectThrows(IllegalStateException::class.java) { run(action(clusterState(), client)) }
    }

    fun `test other failures keep their status`() {
        val client = FakeClient(permissionFailure = IllegalArgumentException("bad request"))

        val e = expectThrows(OpenSearchException::class.java) { run(action(clusterState(), client)) }

        assertEquals(RestStatus.BAD_REQUEST, e.status())
    }

    fun `test an index that does not exist is not found`() {
        expectThrows(IndexNotFoundException::class.java) { run(action(clusterState(withIndex = false))) }
    }

    fun `test the request is paused while the cluster has mixed ISM versions`() {
        pauseExecution()

        assertFailedWith(run(action(clusterState())), "paused")
    }

    fun `test a job owned by a node of another version is not forwarded`() {
        val olderNode = DiscoveryNode("old", buildNewFakeTransportAddress(), Version.V_2_0_0)
        val state = clusterState(nodes = listOf(localNode, olderNode), configPrimary = olderNode to ShardRoutingState.STARTED)

        // forwarding would call the final sendRequest of the mocked transport service and fail
        assertFailedWith(run(action(state)), "paused")
    }

    fun `test a config shard primary that is not started is a 503`() {
        val state = clusterState(configPrimary = localNode to ShardRoutingState.INITIALIZING)

        val e = expectThrows(OpenSearchStatusException::class.java) { run(action(state)) }

        assertEquals(RestStatus.SERVICE_UNAVAILABLE, e.status())
    }

    fun `test an index is not managed when there is no config index`() {
        assertFailedWith(run(action(clusterState(configPrimary = null))), "not being managed")
    }

    fun `test an index is not managed when it has no job document`() {
        assertFailedWith(run(action(clusterState())), "not being managed")
    }

    fun `test a failure to get the job document is a 500`() {
        val client = FakeClient(getFailure = IllegalStateException("config index unavailable"))

        val e = expectThrows(OpenSearchException::class.java) { run(action(clusterState(), client)) }

        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, e.status())
    }

    fun `test a disabled job is reported and not run`() {
        val disabledJob = randomManagedIndexConfig(index = indexName, uuid = indexUuid).copy(enabled = false, jobEnabledTime = null)
        val client = FakeClient(jobDocument = disabledJob.toJsonString())

        assertFailedWith(run(action(clusterState(), client)), "disabled")
        verify(lockService, never()).acquireLock(any(), any(), any())
    }

    fun `test a job whose lock another run holds is reported as already executing`() {
        val enabledJob = randomManagedIndexConfig(index = indexName, uuid = indexUuid).copy(enabled = true, jobEnabledTime = Instant.now())
        val client = FakeClient(jobDocument = enabledJob.toJsonString())
        // no lock: another run holds it
        doAnswer {
            @Suppress("UNCHECKED_CAST")
            (it.arguments[2] as ActionListener<Any?>).onResponse(null)
            null
        }.whenever(lockService).acquireLock(any(), any(), any())

        assertFailedWith(run(action(clusterState(), client)), "already executing")
    }

    fun `test the internal action answers through the channel`() {
        val handlerCaptor = argumentCaptor<TransportRequestHandler<ExecuteManagedIndexRequest>>()
        action(clusterState())
        verify(transportService).registerRequestHandler<ExecuteManagedIndexRequest>(
            eq(ExecuteManagedIndexAction.NODE_ACTION_NAME), eq(ThreadPool.Names.SAME), any<Writeable.Reader<ExecuteManagedIndexRequest>>(),
            handlerCaptor.capture(),
        )
        val channel: TransportChannel = mock()

        handlerCaptor.firstValue.messageReceived(ExecuteManagedIndexRequest(indexName), channel, mock<Task>())

        verify(channel, Mockito.timeout(TIMEOUT_MILLIS)).sendResponse(any<ISMStatusResponse>())
    }

    fun `test the internal action reports an index that does not exist`() {
        val handlerCaptor = argumentCaptor<TransportRequestHandler<ExecuteManagedIndexRequest>>()
        action(clusterState(withIndex = false))
        verify(transportService).registerRequestHandler<ExecuteManagedIndexRequest>(
            eq(ExecuteManagedIndexAction.NODE_ACTION_NAME), eq(ThreadPool.Names.SAME), any<Writeable.Reader<ExecuteManagedIndexRequest>>(),
            handlerCaptor.capture(),
        )
        val channel: TransportChannel = mock()

        handlerCaptor.firstValue.messageReceived(ExecuteManagedIndexRequest(indexName), channel, mock<Task>())

        verify(channel, Mockito.timeout(TIMEOUT_MILLIS)).sendResponse(any<IndexNotFoundException>())
    }

    companion object {
        private const val TIMEOUT_MILLIS = 10_000L
    }
}
