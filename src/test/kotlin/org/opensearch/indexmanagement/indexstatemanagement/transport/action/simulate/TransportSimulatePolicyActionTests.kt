/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.OpenSearchStatusException
import org.opensearch.Version
import org.opensearch.action.admin.indices.rollover.RolloverInfo
import org.opensearch.action.admin.indices.stats.CommonStats
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.get.GetResponse
import org.opensearch.action.get.MultiGetItemResponse
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesArray
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.shard.DocsStats
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.model.Conditions
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

// Kotlin's Dispatchers.IO thread pool keeps idle workers alive after the test suite
// completes.  Suppress the resulting thread-leak warnings rather than fighting the
// scheduler's lifecycle.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class TransportSimulatePolicyActionTests : OpenSearchTestCase() {
    companion object {
        private const val INDEX_NAME = "test-index"
        private const val INDEX_UUID = "test-uuid"
        private const val POLICY_ID = "my-policy"
    }

    // RETURNS_DEEP_STUBS so that transportService.getTaskManager() (called in the
    // TransportAction constructor) returns a stub instead of null.
    private val transportService: TransportService = mock(defaultAnswer = Mockito.RETURNS_DEEP_STUBS)

    // IndexMetadataProvider is final — must be instantiated (not mocked).
    private lateinit var indexMetadataProvider: IndexMetadataProvider

    @Before
    fun setUpIndexMetadataProvider() {
        val providerClusterService: ClusterService = mock()
        whenever(providerClusterService.clusterSettings).thenReturn(
            ClusterSettings(Settings.EMPTY, setOf(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN)),
        )
        indexMetadataProvider = IndexMetadataProvider(Settings.EMPTY, mock(), providerClusterService, mutableMapOf())
    }

    // -------------------------------------------------------------------------
    // Action / execution helpers
    // -------------------------------------------------------------------------

    private fun buildAction(
        client: Client,
        clusterService: ClusterService,
        resolver: IndexNameExpressionResolver = passthroughResolver(),
    ) = TransportSimulatePolicyAction(
        client,
        transportService,
        ActionFilters(emptySet()),
        clusterService,
        NamedXContentRegistry.EMPTY,
        indexMetadataProvider,
        resolver,
    )

    /**
     * A resolver that returns any concrete index name (no wildcards) unchanged.
     * Used by tests that pass explicit index names and don't need pattern expansion.
     */
    private fun passthroughResolver(): IndexNameExpressionResolver {
        val resolver = mock<IndexNameExpressionResolver>()
        whenever(resolver.concreteIndexNames(any(), any(), any<String>())).thenAnswer { invocation ->
            val pattern = invocation.getArgument<String>(2)
            if (pattern.contains('*') || pattern.contains('?')) emptyArray() else arrayOf(pattern)
        }
        return resolver
    }

    /**
     * Calls the public [execute] entry-point and blocks until the listener fires.
     * Rethrows on failure so callers can use try/catch for expected exceptions.
     */
    private fun runSimulate(
        action: TransportSimulatePolicyAction,
        request: SimulatePolicyRequest,
    ): SimulatePolicyResponse {
        val latch = CountDownLatch(1)
        var response: SimulatePolicyResponse? = null
        var failure: Exception? = null

        action.execute(
            request,
            object : ActionListener<SimulatePolicyResponse> {
                override fun onResponse(r: SimulatePolicyResponse) {
                    response = r
                    latch.countDown()
                }

                override fun onFailure(e: Exception) {
                    failure = e
                    latch.countDown()
                }
            },
        )

        assertTrue("Timed out waiting for simulate response", latch.await(5, TimeUnit.SECONDS))
        if (failure != null) throw failure!!
        return response!!
    }

    // -------------------------------------------------------------------------
    // IndexMetadata builder helpers
    //
    // IndexMetadata has final methods — build real instances via IndexMetadata.Builder
    // rather than mocking, following the pattern used in ExtensionsTests.
    // -------------------------------------------------------------------------

    private fun buildIndexMetadata(
        indexName: String = INDEX_NAME,
        indexUUID: String = INDEX_UUID,
        creationDate: Long = Instant.now().minusSeconds(86400L * 10).toEpochMilli(),
        aliasNames: List<String> = emptyList(),
        rolloverInfoMap: Map<String, Long> = emptyMap(),
    ): IndexMetadata {
        var builder =
            IndexMetadata
                .Builder(indexName)
                .settings(
                    settings(Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                        .put(IndexMetadata.SETTING_CREATION_DATE, creationDate)
                        .build(),
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
        aliasNames.forEach { builder = builder.putAlias(AliasMetadata.builder(it).build()) }
        rolloverInfoMap.forEach { (alias, time) ->
            builder = builder.putRolloverInfo(RolloverInfo(alias, emptyList(), time))
        }
        return builder.build()
    }

    // -------------------------------------------------------------------------
    // ClusterService factory helpers
    // -------------------------------------------------------------------------

    private fun clusterServiceWithNoIndex(): ClusterService {
        val clusterState = ClusterState.builder(ClusterName.DEFAULT).build()
        return mock { on { state() } doReturn clusterState }
    }

    private fun clusterServiceWithIndex(
        creationDate: Long = Instant.now().minusSeconds(86400L * 10).toEpochMilli(),
        aliasNames: List<String> = emptyList(),
        rolloverInfoMap: Map<String, Long> = emptyMap(),
    ): ClusterService {
        val indexMetadata = buildIndexMetadata(
            creationDate = creationDate,
            aliasNames = aliasNames,
            rolloverInfoMap = rolloverInfoMap,
        )
        val clusterMetadata = Metadata.builder().put(indexMetadata, false).build()
        val clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(clusterMetadata).build()
        return mock { on { state() } doReturn clusterState }
    }

    // -------------------------------------------------------------------------
    // Client factory helpers
    // -------------------------------------------------------------------------

    /** Client where multiGet returns an empty response → unmanaged index. */
    private fun unmanagedClient(): Client {
        val emptyMget: MultiGetResponse = mock { on { responses } doReturn emptyArray() }
        val client = mock<Client>()
        doAnswer { it.getArgument<ActionListener<MultiGetResponse>>(1).onResponse(emptyMget) }
            .whenever(client).multiGet(any(), any())
        return client
    }

    /** Client where multiGet returns serialised [managedMeta] → managed index. */
    private fun managedClient(managedMeta: ManagedIndexMetaData): Client {
        val bytes = serialisedMeta(managedMeta)
        val innerGet: GetResponse =
            mock {
                on { isExists } doReturn true
                on { sourceAsBytesRef } doReturn bytes
                on { id } doReturn managedIndexMetadataID(INDEX_UUID)
                on { seqNo } doReturn 0L
                on { primaryTerm } doReturn 1L
            }
        val item: MultiGetItemResponse = mock { on { response } doReturn innerGet }
        val mgetResp: MultiGetResponse = mock { on { responses } doReturn arrayOf(item) }
        val client = mock<Client>()
        doAnswer { it.getArgument<ActionListener<MultiGetResponse>>(1).onResponse(mgetResp) }
            .whenever(client).multiGet(any(), any())
        return client
    }

    /** Client that handles multiGet (unmanaged) AND index-level doc/size stats. */
    private fun unmanagedClientWithStats(numDocs: Long, sizeBytes: Long): Client {
        // Use real DocsStats and CommonStats objects to avoid Mockito stubbing issues with
        // DocsStats.count / totalSizeInBytes inside mock { } blocks (those getters are final).
        val realDocsStats = DocsStats.Builder().count(numDocs).totalSizeInBytes(sizeBytes).build()
        val realCommonStats = CommonStats()
        realCommonStats.docs = realDocsStats
        val statsResp: IndicesStatsResponse = mock { on { primaries } doReturn realCommonStats }

        val indicesAdmin: IndicesAdminClient = mock()
        doAnswer { it.getArgument<ActionListener<IndicesStatsResponse>>(1).onResponse(statsResp) }
            .whenever(indicesAdmin).stats(any(), any())
        val adminMock: AdminClient = mock { on { indices() } doReturn indicesAdmin }

        val emptyMget: MultiGetResponse = mock { on { responses } doReturn emptyArray() }
        val client = mock<Client>()
        doAnswer { it.getArgument<ActionListener<MultiGetResponse>>(1).onResponse(emptyMget) }
            .whenever(client).multiGet(any(), any())
        doReturn(adminMock).whenever(client).admin()
        return client
    }

    // -------------------------------------------------------------------------
    // Policy factory helpers
    // -------------------------------------------------------------------------

    /**
     * Policy with a "hot" state (no actions, only [transitions]) plus terminal "warm" and
     * "archive" states to satisfy [Policy.init] validation.
     */
    private fun policyWithTransitions(vararg transitions: Transition): Policy {
        val hot = State(name = "hot", actions = emptyList(), transitions = listOf(*transitions))
        val warm = State(name = "warm", actions = emptyList(), transitions = emptyList())
        val archive = State(name = "archive", actions = emptyList(), transitions = emptyList())
        return Policy(
            id = POLICY_ID,
            description = "test",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now(),
            errorNotification = null,
            defaultState = "hot",
            states = listOf(hot, warm, archive),
        )
    }

    /** Policy whose "hot" state has one action — index is not in the transition phase. */
    private fun policyWithAction(): Policy {
        // Action.type is a val constructor parameter (final getter) — cannot be stubbed
        // by Mockito's subclass mock maker.  Use a concrete anonymous object instead.
        val action =
            object : org.opensearch.indexmanagement.spi.indexstatemanagement.Action("read_only", 0) {
                override fun getSteps(): List<Step> = emptyList()

                override fun getStepToExecute(context: StepContext): Step = throw UnsupportedOperationException("not used in this test")
            }
        val hot = State(name = "hot", actions = listOf(action), transitions = emptyList())
        return Policy(
            id = POLICY_ID,
            description = "test",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now(),
            errorNotification = null,
            defaultState = "hot",
            states = listOf(hot),
        )
    }

    // -------------------------------------------------------------------------
    // Serialisation helper
    // -------------------------------------------------------------------------

    private fun serialisedMeta(meta: ManagedIndexMetaData): BytesReference {
        val builder = XContentFactory.jsonBuilder()
        meta.toXContent(builder, ToXContent.EMPTY_PARAMS, forIndex = true)
        return BytesArray(builder.toString().toByteArray(Charsets.UTF_8))
    }

    // =========================================================================
    // Tests — simulateIndex paths
    // =========================================================================

    fun `test index not found returns error result`() {
        val client: Client = mock()
        val policy = policyWithTransitions(Transition("warm", null))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithNoIndex()), request)

        assertEquals(1, response.simulateResults.size)
        val result = response.simulateResults.first()
        assertEquals(INDEX_NAME, result.indexName)
        assertNull(result.indexUUID)
        assertFalse(result.isManaged)
        assertNotNull(result.error)
        assertTrue("Error should mention index name", result.error!!.contains(INDEX_NAME))
    }

    fun `test state referenced by managed metadata not found in policy returns error`() {
        // Managed metadata says the index is in state "ghost" which the policy does not have.
        val stateStartMillis = Instant.now().toEpochMilli()
        val managedMeta =
            ManagedIndexMetaData(
                index = INDEX_NAME,
                indexUuid = INDEX_UUID,
                policyID = POLICY_ID,
                policySeqNo = null,
                policyPrimaryTerm = null,
                policyCompleted = null,
                rolledOver = null,
                indexCreationDate = null,
                transitionTo = null,
                stateMetaData = StateMetaData(name = "ghost", startTime = stateStartMillis),
                actionMetaData = null,
                stepMetaData = null,
                // Non-null so that toXContent(forIndex=true) writes an object rather than null,
                // avoiding a parse failure in PolicyRetryInfoMetaData.parse().
                policyRetryInfo = PolicyRetryInfoMetaData(failed = false, consumedRetries = 0),
                info = null,
            )
        val client = managedClient(managedMeta)
        // Policy has "hot", "warm", "archive" but not "ghost"
        val policy = policyWithTransitions(Transition("warm", null))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex()), request)

        val result = response.simulateResults.first()
        assertNotNull(result.error)
        assertTrue("Error should mention the missing state", result.error!!.contains("ghost"))
    }

    fun `test unmanaged index with actions shows current action and no transition evaluation`() {
        val client = unmanagedClient()
        val policy = policyWithAction()
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex()), request)

        val result = response.simulateResults.first()
        assertNull(result.error)
        assertFalse(result.isManaged)
        assertNotNull("currentAction should be populated", result.currentAction)
        assertNull("transitionEvaluation should be absent when not in transition phase", result.transitionEvaluation)
        assertNull(result.nextState)
    }

    fun `test multiple indices are simulated independently`() {
        // Cluster state contains INDEX_NAME but not "missing-index".
        val indexMetadata = buildIndexMetadata()
        val clusterMetadata = Metadata.builder().put(indexMetadata, false).build()
        val clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(clusterMetadata).build()
        val clusterService: ClusterService = mock { on { state() } doReturn clusterState }

        val client = unmanagedClient()
        val policy = policyWithTransitions(Transition("warm", null))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME, "missing-index"), null, policy)

        val response = runSimulate(buildAction(client, clusterService), request)

        assertEquals(2, response.simulateResults.size)
        val found = response.simulateResults.find { it.indexName == INDEX_NAME }!!
        val missing = response.simulateResults.find { it.indexName == "missing-index" }!!
        assertNull(found.error)
        assertNotNull(missing.error)
    }

    fun `test wildcard pattern expands to matching indices`() {
        // Resolver expands "test-*" to the single known index
        val resolver = mock<IndexNameExpressionResolver>()
        whenever(resolver.concreteIndexNames(any(), any(), eq("test-*")))
            .thenReturn(arrayOf(INDEX_NAME))

        val client = unmanagedClient()
        val policy = policyWithTransitions(Transition("warm", null))
        val request = SimulatePolicyRequest(listOf("test-*"), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex(), resolver), request)

        assertEquals(1, response.simulateResults.size)
        assertEquals(INDEX_NAME, response.simulateResults.first().indexName)
        assertNull(response.simulateResults.first().error)
    }

    fun `test wildcard pattern matching no indices returns empty results`() {
        // Resolver returns nothing for the pattern — silently dropped
        val resolver = mock<IndexNameExpressionResolver>()
        whenever(resolver.concreteIndexNames(any(), any(), eq("ghost-*")))
            .thenReturn(emptyArray())

        val client = unmanagedClient()
        val policy = policyWithTransitions(Transition("warm", null))
        val request = SimulatePolicyRequest(listOf("ghost-*"), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex(), resolver), request)

        assertTrue(response.simulateResults.isEmpty())
    }

    // =========================================================================
    // Tests — evaluateTransition condition coverage
    // =========================================================================

    fun `test unconditional transition always fires`() {
        val client = unmanagedClient()
        val policy = policyWithTransitions(Transition("warm", null))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex()), request)

        val result = response.simulateResults.first()
        assertNull(result.error)
        assertEquals(1, result.transitionEvaluation!!.size)
        val t = result.transitionEvaluation!!.first()
        assertTrue(t.conditionMet)
        assertEquals("unconditional", t.conditionType)
        assertNull(t.currentValue)
        assertNull(t.requiredValue)
        assertEquals("warm", result.nextState)
    }

    fun `test min index age condition met`() {
        val client = unmanagedClient()
        // Index is 10 days old; condition requires 7 days → met
        val clusterService = clusterServiceWithIndex(
            creationDate = Instant.now().minusSeconds(86400L * 10).toEpochMilli(),
        )
        val policy = policyWithTransitions(Transition("warm", Conditions(indexAge = TimeValue.timeValueDays(7))))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterService), request)

        val t = response.simulateResults.first().transitionEvaluation!!.first()
        assertTrue(t.conditionMet)
        assertEquals(Conditions.MIN_INDEX_AGE_FIELD, t.conditionType)
        assertNotNull(t.currentValue)
        assertNotNull(t.requiredValue)
        assertEquals("warm", response.simulateResults.first().nextState)
    }

    fun `test min index age condition not met`() {
        val client = unmanagedClient()
        // Index is 3 days old; condition requires 7 days → not met
        val clusterService = clusterServiceWithIndex(
            creationDate = Instant.now().minusSeconds(86400L * 3).toEpochMilli(),
        )
        val policy = policyWithTransitions(Transition("warm", Conditions(indexAge = TimeValue.timeValueDays(7))))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterService), request)

        val t = response.simulateResults.first().transitionEvaluation!!.first()
        assertFalse(t.conditionMet)
        assertEquals(Conditions.MIN_INDEX_AGE_FIELD, t.conditionType)
        assertNull(response.simulateResults.first().nextState)
    }

    fun `test min doc count condition fetches stats and evaluates`() {
        // 1 000 docs; condition requires 500 → met
        val client = unmanagedClientWithStats(numDocs = 1000L, sizeBytes = 0L)
        val policy = policyWithTransitions(Transition("warm", Conditions(docCount = 500L)))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex()), request)

        val t = response.simulateResults.first().transitionEvaluation!!.first()
        assertEquals(Conditions.MIN_DOC_COUNT_FIELD, t.conditionType)
        assertEquals("1000", t.currentValue)
        assertEquals("500", t.requiredValue)
        assertTrue(t.conditionMet)
    }

    fun `test min size condition fetches stats and evaluates`() {
        val sizeBytes = 5L * 1024 * 1024 * 1024 // 5 GB — exceeds 1 GB condition
        val client = unmanagedClientWithStats(numDocs = 0L, sizeBytes = sizeBytes)
        val policy = policyWithTransitions(
            Transition("warm", Conditions(size = ByteSizeValue(1L * 1024 * 1024 * 1024))),
        )
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex()), request)

        val t = response.simulateResults.first().transitionEvaluation!!.first()
        assertEquals(Conditions.MIN_SIZE_FIELD, t.conditionType)
        assertNotNull(t.currentValue)
        assertNotNull(t.requiredValue)
        assertTrue(t.conditionMet)
    }

    fun `test min rollover age with no rollover history returns not met`() {
        val client = unmanagedClient()
        val policy = policyWithTransitions(Transition("warm", Conditions(rolloverAge = TimeValue.timeValueDays(1))))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex()), request)

        val t = response.simulateResults.first().transitionEvaluation!!.first()
        assertFalse(t.conditionMet)
        assertEquals(Conditions.MIN_ROLLOVER_AGE_FIELD, t.conditionType)
        assertEquals("index has never been rolled over", t.currentValue)
    }

    fun `test min rollover age with old enough rollover returns met`() {
        // Rolled over 3 days ago; condition requires 1 day → met
        val rolloverTime = Instant.now().minusSeconds(86400L * 3).toEpochMilli()
        val client = unmanagedClient()
        val clusterService = clusterServiceWithIndex(rolloverInfoMap = mapOf("my-alias" to rolloverTime))
        val policy = policyWithTransitions(Transition("warm", Conditions(rolloverAge = TimeValue.timeValueDays(1))))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterService), request)

        val t = response.simulateResults.first().transitionEvaluation!!.first()
        assertTrue(t.conditionMet)
        assertEquals(Conditions.MIN_ROLLOVER_AGE_FIELD, t.conditionType)
        assertNotNull(t.currentValue)
    }

    fun `test no alias condition with zero aliases returns met`() {
        val client = unmanagedClient()
        val policy = policyWithTransitions(Transition("warm", Conditions(noAlias = true)))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex(aliasNames = emptyList())), request)

        val t = response.simulateResults.first().transitionEvaluation!!.first()
        assertEquals(Conditions.NO_ALIAS_FIELD, t.conditionType)
        assertTrue(t.conditionMet)
        assertEquals("no aliases", t.currentValue)
    }

    fun `test no alias condition with aliases present returns not met`() {
        val client = unmanagedClient()
        val clusterService = clusterServiceWithIndex(aliasNames = listOf("alias-0", "alias-1"))
        val policy = policyWithTransitions(Transition("warm", Conditions(noAlias = true)))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterService), request)

        val t = response.simulateResults.first().transitionEvaluation!!.first()
        assertEquals(Conditions.NO_ALIAS_FIELD, t.conditionType)
        assertFalse(t.conditionMet)
        assertTrue("currentValue should include alias count", t.currentValue!!.contains("2"))
    }

    fun `test min state age with no state start time returns not met with unknown message`() {
        // Unmanaged index → stateStartTime is null in the transition context
        val client = unmanagedClient()
        val policy = policyWithTransitions(Transition("warm", Conditions(minStateAge = TimeValue.timeValueDays(1))))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex()), request)

        val t = response.simulateResults.first().transitionEvaluation!!.first()
        assertEquals(Conditions.MIN_STATE_AGE_FIELD, t.conditionType)
        assertFalse(t.conditionMet)
        assertTrue("currentValue should describe unknown state start", t.currentValue!!.contains("unknown"))
    }

    fun `test min state age with known state start time returns met`() {
        // State started 2 days ago; condition requires 1 day → met
        val stateStartMillis = Instant.now().minusSeconds(86400L * 2).toEpochMilli()
        val managedMeta =
            ManagedIndexMetaData(
                index = INDEX_NAME,
                indexUuid = INDEX_UUID,
                policyID = POLICY_ID,
                policySeqNo = null,
                policyPrimaryTerm = null,
                policyCompleted = null,
                rolledOver = null,
                indexCreationDate = null,
                transitionTo = null,
                stateMetaData = StateMetaData(name = "hot", startTime = stateStartMillis),
                actionMetaData = null,
                stepMetaData = null,
                // Non-null so that toXContent(forIndex=true) writes an object rather than null,
                // avoiding a parse failure in PolicyRetryInfoMetaData.parse().
                policyRetryInfo = PolicyRetryInfoMetaData(failed = false, consumedRetries = 0),
                info = null,
            )
        val client = managedClient(managedMeta)
        val policy = policyWithTransitions(Transition("warm", Conditions(minStateAge = TimeValue.timeValueDays(1))))
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterServiceWithIndex()), request)

        val result = response.simulateResults.first()
        assertNull(result.error)
        assertTrue(result.isManaged)
        val t = result.transitionEvaluation!!.first()
        assertEquals(Conditions.MIN_STATE_AGE_FIELD, t.conditionType)
        assertTrue(t.conditionMet)
        assertNotNull(t.currentValue)
        assertEquals("warm", result.nextState)
    }

    fun `test first matching transition wins when multiple exist`() {
        val client = unmanagedClient()
        // Index is 10 days old; first transition needs 30 d (not met), second needs 7 d (met)
        val clusterService = clusterServiceWithIndex(
            creationDate = Instant.now().minusSeconds(86400L * 10).toEpochMilli(),
        )
        val policy = policyWithTransitions(
            Transition("archive", Conditions(indexAge = TimeValue.timeValueDays(30))),
            Transition("warm", Conditions(indexAge = TimeValue.timeValueDays(7))),
        )
        val request = SimulatePolicyRequest(listOf(INDEX_NAME), null, policy)

        val response = runSimulate(buildAction(client, clusterService), request)

        val evals = response.simulateResults.first().transitionEvaluation!!
        assertEquals(2, evals.size)
        assertFalse("30-day transition should not be met", evals[0].conditionMet)
        assertTrue("7-day transition should be met", evals[1].conditionMet)
        assertEquals("warm", response.simulateResults.first().nextState)
    }

    // =========================================================================
    // Tests — resolvePolicy path
    // =========================================================================

    fun `test policy not found by id propagates failure`() {
        val notFoundGet: GetResponse = mock { on { isExists } doReturn false }
        val client = mock<Client>()
        doAnswer { it.getArgument<ActionListener<GetResponse>>(1).onResponse(notFoundGet) }
            .whenever(client).get(any(), any())

        val request = SimulatePolicyRequest(listOf(INDEX_NAME), POLICY_ID, null)

        try {
            runSimulate(buildAction(client, clusterServiceWithNoIndex()), request)
            fail("Expected OpenSearchStatusException for missing policy")
        } catch (e: OpenSearchStatusException) {
            assertTrue("Exception should mention policy id", e.message!!.contains(POLICY_ID))
        }
    }
}
