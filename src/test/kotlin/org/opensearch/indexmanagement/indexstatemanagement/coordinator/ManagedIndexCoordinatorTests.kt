/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator
import org.opensearch.indexmanagement.indexstatemanagement.MetadataService
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
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
    private lateinit var coordinator: ManagedIndexCoordinator

    private lateinit var discoveryNode: DiscoveryNode

    @Before
    @Throws(Exception::class)
    fun setup() {
        client = Mockito.mock(Client::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        indexManagementIndices = Mockito.mock(IndexManagementIndices::class.java)
        metadataService = Mockito.mock(MetadataService::class.java)

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
        settingSet.add(ManagedIndexSettings.METADATA_SERVICE_ENABLED)
        settingSet.add(ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT)
        settingSet.add(ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS)

        val clusterSettings = ClusterSettings(settings, settingSet)
        val originClusterService: ClusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings)
        clusterService = Mockito.spy(originClusterService)

        coordinator = ManagedIndexCoordinator(settings, client, clusterService, threadPool, indexManagementIndices, metadataService)
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

    fun `test on master`() {
        coordinator.onMaster()
        Mockito.verify(threadPool, Mockito.times(2)).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())
    }

    fun `test off master`() {
        val cancellable = Mockito.mock(Scheduler.Cancellable::class.java)

        coordinator.offMaster()
        Mockito.verify(cancellable, Mockito.times(0)).cancel()

        Mockito.`when`(threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable)
        coordinator.initBackgroundSweep()
        coordinator.offMaster()
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
