/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.runner

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.routing.Preference
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.env.Environment
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementHistory
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.SkipExecution
import org.opensearch.indexmanagement.indexstatemanagement.randomManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.script.ScriptService
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ManagedIndexRunnerTests : OpenSearchTestCase() {
    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var environment: Environment
    private lateinit var indexStateManagementHistory: IndexStateManagementHistory
    private lateinit var skipFlag: SkipExecution
    private lateinit var runner: ManagedIndexRunner

    private lateinit var settings: Settings
    private lateinit var discoveryNode: DiscoveryNode
    private lateinit var threadPool: ThreadPool

    @Before
    @Throws(Exception::class)
    fun setup() {
        clusterService = Mockito.mock(ClusterService::class.java)
        xContentRegistry = Mockito.mock(NamedXContentRegistry::class.java)
        scriptService = Mockito.mock(ScriptService::class.java)
        environment = Mockito.mock(Environment::class.java)
        indexStateManagementHistory = Mockito.mock(IndexStateManagementHistory::class.java)
        skipFlag = Mockito.mock(SkipExecution::class.java)

        threadPool = Mockito.mock(ThreadPool::class.java)
        settings = Settings.builder().build()
        discoveryNode = DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT)
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(ManagedIndexSettings.SWEEP_PERIOD)
        settingSet.add(ManagedIndexSettings.JITTER)
        settingSet.add(ManagedIndexSettings.JOB_INTERVAL)
        settingSet.add(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED)
        settingSet.add(ManagedIndexSettings.ACTION_VALIDATION_ENABLED)
        settingSet.add(ManagedIndexSettings.ALLOW_LIST)
        settingSet.add(ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER)
        settingSet.add(ManagedIndexSettings.JOB_DOCUMENT_CHECK_ENABLED)
        val clusterSettings = ClusterSettings(settings, settingSet)
        val originClusterService: ClusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings)
        clusterService = Mockito.spy(originClusterService)

        Mockito.`when`(environment.settings()).thenReturn(settings)
        client = Mockito.mock(Client::class.java)

        runner =
            ManagedIndexRunner
                .registerClusterService(clusterService)
                .registerClient(client)
                .registerNamedXContentRegistry(xContentRegistry)
                .registerScriptService(scriptService)
                .registerSettings(environment.settings())
                .registerConsumers()
                .registerHistoryIndex(indexStateManagementHistory)
                .registerSkipFlag(skipFlag)
    }

    fun `test job document check allows initialization when the job document exists`() {
        val managedIndexConfig = randomManagedIndexConfig()
        val requests = mockJobDocumentGet(exists = true)

        val exists = runBlocking { runner.jobDocumentExists(managedIndexConfig) }

        assertTrue("run should proceed when the job document exists", exists)
        assertEquals("exactly one GET expected", 1, requests.size)
    }

    fun `test job document check uses a realtime primary GET without source`() {
        val managedIndexConfig = randomManagedIndexConfig()
        val requests = mockJobDocumentGet(exists = true)

        runBlocking { runner.jobDocumentExists(managedIndexConfig) }

        val request = requests.single()
        assertEquals(INDEX_MANAGEMENT_INDEX, request.index())
        assertEquals(managedIndexConfig.indexUuid, request.id())
        assertEquals("job doc and metadata doc share the routing", managedIndexConfig.indexUuid, request.routing())
        assertTrue("lookup must be realtime to see unrefreshed writes", request.realtime())
        assertEquals("lookup must be served by the primary copy", Preference.PRIMARY.type(), request.preference())
        assertFalse("existence check must not fetch the source", request.fetchSourceContext().fetchSource())
    }

    fun `test job document check skips initialization when the job document is gone`() {
        val managedIndexConfig = randomManagedIndexConfig()
        mockJobDocumentGet(exists = false)

        val exists = runBlocking { runner.jobDocumentExists(managedIndexConfig) }

        assertFalse("run must be skipped when the job document no longer exists", exists)
    }

    fun `test job document check aborts the run when the lookup fails`() {
        val managedIndexConfig = randomManagedIndexConfig()
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<GetResponse>>(1)
            listener.onFailure(RuntimeException("config index unavailable"))
        }.whenever(client).get(any(), any())

        val exists = runBlocking { runner.jobDocumentExists(managedIndexConfig) }

        assertFalse("run must be aborted when the job document cannot be verified", exists)
    }

    fun `test job document check is bypassed when disabled`() {
        val managedIndexConfig = randomManagedIndexConfig()
        mockJobDocumentGet(exists = false)
        applyJobDocumentCheckSetting(false)
        try {
            val exists = runBlocking { runner.jobDocumentExists(managedIndexConfig) }

            assertTrue("disabled check must fall back to the previous behaviour", exists)
            verify(client, never()).get(any(), any())
        } finally {
            applyJobDocumentCheckSetting(true)
        }
    }

    fun `test job document check follows dynamic setting updates`() {
        val managedIndexConfig = randomManagedIndexConfig()
        mockJobDocumentGet(exists = false)

        applyJobDocumentCheckSetting(false)
        assertTrue(runBlocking { runner.jobDocumentExists(managedIndexConfig) })
        verify(client, never()).get(any(), any())

        applyJobDocumentCheckSetting(true)
        assertFalse(runBlocking { runner.jobDocumentExists(managedIndexConfig) })
        verify(client, times(1)).get(any(), any())
    }

    private fun applyJobDocumentCheckSetting(enabled: Boolean) {
        clusterService.clusterSettings.applySettings(
            Settings.builder().put(ManagedIndexSettings.JOB_DOCUMENT_CHECK_ENABLED.key, enabled).build(),
        )
    }

    /** Answers every client.get with a response whose isExists is [exists] and records the requests it received. */
    private fun mockJobDocumentGet(exists: Boolean): MutableList<GetRequest> {
        val requests = mutableListOf<GetRequest>()
        val getResponse: GetResponse = mock()
        whenever(getResponse.isExists).doReturn(exists)
        doAnswer { invocation ->
            requests.add(invocation.getArgument<GetRequest>(0))
            val listener = invocation.getArgument<ActionListener<GetResponse>>(1)
            listener.onResponse(getResponse)
        }.whenever(client).get(any(), any())
        return requests
    }
}
