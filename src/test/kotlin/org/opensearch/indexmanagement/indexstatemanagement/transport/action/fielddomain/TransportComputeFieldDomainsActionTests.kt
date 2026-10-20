/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.fielddomain

import com.carrotsearch.randomizedtesting.ThreadFilter
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import org.mockito.Mockito
import org.opensearch.OpenSearchSecurityException
import org.opensearch.Version
import org.opensearch.action.ActionRequest
import org.opensearch.action.admin.indices.refresh.RefreshRequest
import org.opensearch.action.admin.indices.refresh.RefreshResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.action.support.ActionFilter
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.PlainActionFuture
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.MappingMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.CheckedRunnable
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.core.index.Index
import org.opensearch.identity.PluginSubject
import org.opensearch.index.fielddomain.DateRangeFieldDomain
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainConfig
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainSearchContext
import org.opensearch.search.DocValueFormat
import org.opensearch.search.SearchHit
import org.opensearch.search.SearchHits
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import java.security.Principal
import java.util.Collections
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@ThreadLeakFilters(filters = [SharedCoroutineWorkerFilter::class])
class TransportComputeFieldDomainsActionTests : OpenSearchTestCase() {
    private val threadContext = ThreadContext(Settings.EMPTY)
    private val client = mock<Client>()
    private val indicesClient = mock<IndicesAdminClient>()
    private val clusterService = mock<ClusterService>()
    private val transportService = mock<TransportService>(defaultAnswer = Mockito.RETURNS_DEEP_STUBS)
    private val events = Collections.synchronizedList(mutableListOf<String>())
    private val callbacks = Executors.newSingleThreadExecutor()
    private val index = Index("logs-000001", "index-uuid")

    override fun setUp() {
        super.setUp()
        threadContext.putTransient(IDENTITY, POLICY)
        val threadPool = mock<ThreadPool> { on { threadContext } doReturn this@TransportComputeFieldDomainsActionTests.threadContext }
        val adminClient = mock<AdminClient> { on { indices() } doReturn indicesClient }
        whenever(client.threadPool()).thenReturn(threadPool)
        whenever(client.admin()).thenReturn(adminClient)
        whenever(clusterService.state()).thenReturn(state())
        FieldDomainSearchContext.assignSubject(object : PluginSubject {
            override fun getPrincipal(): Principal = Principal { PLUGIN }

            override fun <E : Exception> runAs(runnable: CheckedRunnable<E>) {
                threadContext.stashContext().use {
                    threadContext.putTransient(IDENTITY, PLUGIN)
                    runnable.run()
                }
            }
        })
    }

    override fun tearDown() {
        try {
            callbacks.shutdown()
            assertTrue(callbacks.awaitTermination(10, TimeUnit.SECONDS))
        } finally {
            super.tearDown()
        }
    }

    fun `test authorization precedes caller refresh and plugin searches`() {
        configureResponses()
        val response = execute()
        val domain = response.fieldDomains().single() as DateRangeFieldDomain
        assertEquals("1", domain.min())
        assertEquals("2", domain.max())
        assertTrue(domain.finalized())
        assertEquals(listOf("authorize", "refresh", "search", "search", "response"), events)
        assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
    }

    fun `test denial prevents all computation even when the field is empty`() {
        configureResponses(empty = true)
        expectThrows(OpenSearchSecurityException::class.java) { execute(allow = false) }
        assertEquals(listOf("authorize", "failure"), events)
        verify(indicesClient, never()).refresh(any(), any())
        verify(client, never()).search(any(), any())
    }

    fun `test asynchronous callbacks preserve caller identity across coroutine resumes`() {
        configureResponses(async = true)
        assertEquals(1, execute().fieldDomains().size)
        assertEquals(listOf("authorize", "refresh", "search", "search", "response"), events)
        assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
    }

    fun `test asynchronous failure restores caller identity`() {
        configureResponses(async = true, refreshFailure = OpenSearchSecurityException("refresh denied"))
        expectThrows(OpenSearchSecurityException::class.java) { execute() }
        assertEquals(listOf("authorize", "refresh", "failure"), events)
        verify(client, never()).search(any(), any())
    }

    fun `test authorized empty field is computed without a second search`() {
        configureResponses(empty = true)
        assertTrue(execute().fieldDomains().isEmpty())
        assertEquals(listOf("authorize", "refresh", "search", "response"), events)
    }

    fun `test refresh denial prevents privileged searches`() {
        configureResponses(refreshFailure = OpenSearchSecurityException("refresh denied"))
        expectThrows(OpenSearchSecurityException::class.java) { execute() }
        assertEquals(listOf("authorize", "refresh", "failure"), events)
        verify(client, never()).search(any(), any())
    }

    fun `test stale index uuid is rejected before refresh`() {
        expectThrows(IllegalArgumentException::class.java) { execute(target = Index(index.name, "stale")) }
        verify(indicesClient, never()).refresh(any(), any())
        verify(client, never()).search(any(), any())
    }

    fun `test write block required even when called outside ISM`() {
        whenever(clusterService.state()).thenReturn(state(blocked = false))
        expectThrows(IllegalStateException::class.java) { execute() }
        verify(indicesClient, never()).refresh(any(), any())
        verify(client, never()).search(any(), any())
    }

    fun `test unsupported calculator rejected before refresh`() {
        expectThrows(IllegalArgumentException::class.java) { execute(type = "arbitrary_search") }
        verify(indicesClient, never()).refresh(any(), any())
        verify(client, never()).search(any(), any())
    }

    fun `test settings changes during calculation discard the results`() {
        configureResponses()
        whenever(clusterService.state()).thenReturn(state(), state(settingsVersion = 2L))
        expectThrows(IllegalStateException::class.java) { execute() }
        assertEquals(listOf("authorize", "refresh", "search", "search", "failure"), events)
    }

    private fun execute(
        allow: Boolean = true,
        target: Index = index,
        type: String = DateRangeFieldDomain.TYPE,
    ): ComputeFieldDomainsResponse {
        val filter = object : ActionFilter.Simple() {
            override fun order(): Int = 0

            override fun apply(action: String, request: ActionRequest, listener: ActionListener<*>): Boolean {
                assertEquals(ComputeFieldDomainsAction.NAME, action)
                assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
                assertArrayEquals(arrayOf(index.name), (request as ComputeFieldDomainsRequest).indices())
                events.add("authorize")
                if (!allow) listener.onFailure(OpenSearchSecurityException("computation denied"))
                return allow
            }
        }
        val action = TransportComputeFieldDomainsAction(transportService, ActionFilters(setOf(filter)), client, clusterService)
        val future = PlainActionFuture<ComputeFieldDomainsResponse>()
        action.execute(
            mock(), ComputeFieldDomainsRequest(target, listOf(FieldDomainConfig("@timestamp", type))),
            object : ActionListener<ComputeFieldDomainsResponse> {
                override fun onResponse(response: ComputeFieldDomainsResponse) {
                    assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
                    events.add("response")
                    future.onResponse(response)
                }

                override fun onFailure(e: Exception) {
                    assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
                    events.add("failure")
                    future.onFailure(e)
                }
            },
        )
        return future.actionGet("10s")
    }

    private fun configureResponses(empty: Boolean = false, refreshFailure: Exception? = null, async: Boolean = false) {
        doAnswer { invocation ->
            assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
            assertArrayEquals(arrayOf(index.name), invocation.getArgument<RefreshRequest>(0).indices())
            events.add("refresh")
            val listener = invocation.getArgument<ActionListener<RefreshResponse>>(1)
            val complete = Runnable {
                if (refreshFailure == null) listener.onResponse(mock()) else listener.onFailure(refreshFailure)
            }
            if (async) callbacks.execute(complete) else complete.run()
            null
        }.whenever(indicesClient).refresh(any(), any())
        var boundary = 0L
        doAnswer { invocation ->
            assertEquals(PLUGIN, threadContext.getTransient<String>(IDENTITY))
            assertArrayEquals(arrayOf(index.name), invocation.getArgument<SearchRequest>(0).indices())
            events.add("search")
            val hit = SearchHit(0)
            hit.sortValues(arrayOf<Any>(++boundary), arrayOf(DocValueFormat.RAW))
            val hits = SearchHits(if (empty) emptyArray() else arrayOf(hit), null, 0.0F)
            val response = mock<SearchResponse> {
                on { getHits() } doReturn hits
                on { shardFailures } doReturn ShardSearchFailure.EMPTY_ARRAY
            }
            val complete = Runnable { invocation.getArgument<ActionListener<SearchResponse>>(1).onResponse(response) }
            if (async) callbacks.execute(complete) else complete.run()
            null
        }.whenever(client).search(any(), any())
    }

    private fun state(blocked: Boolean = true, settingsVersion: Long = 1L): ClusterState {
        val metadata = IndexMetadata.builder(index.name)
            .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, index.uuid).put(IndexMetadata.SETTING_BLOCKS_WRITE, blocked))
            .settingsVersion(settingsVersion).numberOfShards(1).numberOfReplicas(0)
            .putMapping(MappingMetadata("_doc", mapOf("properties" to mapOf("@timestamp" to mapOf("type" to "date")))))
            .build()
        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(metadata, false)).build()
    }

    companion object {
        private const val IDENTITY = "identity"
        private const val POLICY = "policy"
        private const val PLUGIN = "plugin"
    }
}

/** Dispatchers.IO workers belong to the shared Kotlin runtime, not to an individual action or test. */
class SharedCoroutineWorkerFilter : ThreadFilter {
    override fun reject(thread: Thread): Boolean = thread.name.startsWith("DefaultDispatcher-worker-") && thread.stackTrace.any {
        it.className.startsWith("kotlinx.coroutines.scheduling.CoroutineScheduler")
    }
}
