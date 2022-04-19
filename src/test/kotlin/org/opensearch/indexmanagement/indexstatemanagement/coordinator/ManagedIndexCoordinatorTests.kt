/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.coordinator

import org.junit.Before
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.client.Client
import org.opensearch.cluster.OpenSearchAllocationTestCase
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator
import org.opensearch.indexmanagement.indexstatemanagement.MetadataService
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.migration.ISMTemplateService
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool

class ManagedIndexCoordinatorTests : OpenSearchAllocationTestCase() {

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var threadPool: ThreadPool
    private lateinit var settings: Settings

    private lateinit var indexManagementIndices: IndexManagementIndices
    private lateinit var metadataService: MetadataService
    private lateinit var templateService: ISMTemplateService
    private lateinit var coordinator: ManagedIndexCoordinator
    private lateinit var indexMetadataProvider: IndexMetadataProvider

    private lateinit var discoveryNode: DiscoveryNode

    @Before
    @Throws(Exception::class)
    fun setup() {
        client = Mockito.mock(Client::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        indexManagementIndices = Mockito.mock(IndexManagementIndices::class.java)
        metadataService = Mockito.mock(MetadataService::class.java)
        templateService = Mockito.mock(ISMTemplateService::class.java)

        val namedXContentRegistryEntries = arrayListOf<NamedXContentRegistry.Entry>()
        xContentRegistry = NamedXContentRegistry(namedXContentRegistryEntries)

        settings = Settings.builder().build()

        discoveryNode = DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), Version.CURRENT)

        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(ManagedIndexSettings.SWEEP_PERIOD)
        settingSet.add(ManagedIndexSettings.JITTER)
        settingSet.add(ManagedIndexSettings.JOB_INTERVAL)
        settingSet.add(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED)
        settingSet.add(ManagedIndexSettings.METADATA_SERVICE_STATUS)
        settingSet.add(ManagedIndexSettings.TEMPLATE_MIGRATION_CONTROL)
        settingSet.add(ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT)
        settingSet.add(ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS)
        settingSet.add(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN)

        val clusterSettings = ClusterSettings(settings, settingSet)
        val originClusterService: ClusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings)
        clusterService = Mockito.spy(originClusterService)
        indexMetadataProvider = IndexMetadataProvider(settings, client, clusterService, mutableMapOf())
        coordinator = ManagedIndexCoordinator(
            settings, client, clusterService, threadPool, indexManagementIndices, metadataService,
            templateService, indexMetadataProvider
        )
    }

    fun `test after start`() {
        coordinator.afterStart()
        Mockito.verify(threadPool, Mockito.times(2)).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())
    }

    fun `test before stop`() {
        val cancellable = Mockito.mock(Scheduler.Cancellable::class.java)

        coordinator.beforeStop()
        Mockito.verify(cancellable, Mockito.times(0)).cancel()

        Mockito.`when`(threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable)
        coordinator.initBackgroundSweep()
        coordinator.beforeStop()
        Mockito.verify(cancellable).cancel()
    }

    fun `test on cluster manager`() {
        coordinator.onClusterManager()
        Mockito.verify(threadPool, Mockito.times(3)).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())
    }

    fun `test off cluster manager`() {
        val cancellable = Mockito.mock(Scheduler.Cancellable::class.java)

        coordinator.offClusterManager()
        Mockito.verify(cancellable, Mockito.times(0)).cancel()

        Mockito.`when`(threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable)
        coordinator.initBackgroundSweep()
        coordinator.offClusterManager()
        Mockito.verify(cancellable).cancel()
    }

    fun `test init background sweep`() {
        val cancellable = Mockito.mock(Scheduler.Cancellable::class.java)
        Mockito.`when`(threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable)

        coordinator.initBackgroundSweep()
        Mockito.verify(threadPool).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())

        coordinator.initBackgroundSweep()
        Mockito.verify(cancellable).cancel()
        Mockito.verify(threadPool, Mockito.times(2)).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())
    }

    private fun <T> any(): T {
        Mockito.any<T>()
        return uninitialized()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> uninitialized(): T = null as T
}
