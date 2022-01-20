/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.runner

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.client.Client
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.env.Environment
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementHistory
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.SkipExecution
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.script.ScriptService
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool

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
        settingSet.add(ManagedIndexSettings.ALLOW_LIST)
        val clusterSettings = ClusterSettings(settings, settingSet)
        val originClusterService: ClusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings)
        clusterService = Mockito.spy(originClusterService)

        Mockito.`when`(environment.settings()).thenReturn(settings)

        runner = ManagedIndexRunner
            .registerClusterService(clusterService)
            .registerNamedXContentRegistry(xContentRegistry)
            .registerScriptService(scriptService)
            .registerSettings(environment.settings())
            .registerConsumers()
            .registerHistoryIndex(indexStateManagementHistory)
            .registerSkipFlag(skipFlag)
    }
}
