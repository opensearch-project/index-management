/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.junit.Before
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.spi.indexstatemanagement.IndexMetadataService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.rest.OpenSearchRestTestCase

class IndexMetadataProviderTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val client: Client = mock()
    private val settings: Settings = Settings.EMPTY
    private val services = mutableMapOf<String, IndexMetadataService>()

    @Before
    fun `setup settings`() {
        whenever(clusterService.clusterSettings).doReturn(ClusterSettings(Settings.EMPTY, setOf(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN)))
    }

    fun `test security index and kibana should not be manageable`() {
        val indexEvaluator = IndexMetadataProvider(settings, client, clusterService, services)
        assertTrue("Should not manage security index", indexEvaluator.isUnManageableIndex(".opendistro_security"))
        assertTrue("Should not manage kibana index", indexEvaluator.isUnManageableIndex(".kibana_1"))
        assertTrue("Should not manage kibana index", indexEvaluator.isUnManageableIndex(".kibana"))
        assertTrue("Should not manage kibana index", indexEvaluator.isUnManageableIndex(".kibana_20"))
        assertTrue("Should not manage kibana index", indexEvaluator.isUnManageableIndex(".kibana_022"))
        assertTrue(
            "Should not manage index management config index",
            indexEvaluator.isUnManageableIndex(
                IndexManagementPlugin
                    .INDEX_MANAGEMENT_INDEX
            )
        )
        assertTrue("Should not manage kibana index", indexEvaluator.isUnManageableIndex(".kibana_1242142_user"))

        val randomIndex = OpenSearchRestTestCase.randomAlphaOfLength(OpenSearchRestTestCase.randomIntBetween(1, 20))
        assertFalse("Should manage non kibana and security indices", indexEvaluator.isUnManageableIndex(randomIndex))
    }
}
