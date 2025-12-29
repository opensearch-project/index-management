/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.monitor.jvm.JvmService
import org.opensearch.test.OpenSearchTestCase

class ValidateSearchOnlyTests : OpenSearchTestCase() {
    private val settings: Settings = Settings.EMPTY
    private val jvmService: JvmService = mock()

    fun `test validation passes when index exists and is not in search-only mode`() {
        val clusterService = getClusterService("test-index", false)
        val validate = ValidateSearchOnly(settings, clusterService, jvmService)

        val result = validate.execute("test-index")

        assertEquals("Validation status should be PASSED", Validate.ValidationStatus.PASSED, result.validationStatus)
        assertTrue("Message should indicate validation passed", result.validationMessage!!.contains("passed"))
    }

    fun `test validation fails when index does not exist`() {
        val clusterService = getClusterService(null, false)
        val validate = ValidateSearchOnly(settings, clusterService, jvmService)

        val result = validate.execute("non-existent-index")

        assertEquals("Validation status should be FAILED", Validate.ValidationStatus.FAILED, result.validationStatus)
        assertTrue("Message should indicate no such index", result.validationMessage!!.contains("No such index"))
    }

    fun `test validation fails when index is already in search-only mode`() {
        val clusterService = getClusterService("test-index", true)
        val validate = ValidateSearchOnly(settings, clusterService, jvmService)

        val result = validate.execute("test-index")

        assertEquals("Validation status should be FAILED", Validate.ValidationStatus.FAILED, result.validationStatus)
        assertTrue("Message should indicate already in search-only mode", result.validationMessage!!.contains("already"))
    }

    private fun getClusterService(indexName: String?, isSearchOnly: Boolean): ClusterService {
        val clusterSettings = Settings.builder()
            .put("cluster.remote_store.enabled", true)
            .build()
        val metadata: Metadata = if (indexName != null) {
            val indexSettings = Settings.builder()
                .put("index.blocks.search_only", isSearchOnly)
                .put("index.replication.type", "SEGMENT")
                .put("index.number_of_search_only_replicas", 1)
                .build()
            val indexMetadata: IndexMetadata = mock {
                on { settings } doReturn indexSettings
            }
            mock {
                on { index(indexName) } doReturn indexMetadata
                on { settings() } doReturn clusterSettings
            }
        } else {
            mock {
                on { index(org.mockito.ArgumentMatchers.anyString()) } doReturn null
                on { settings() } doReturn clusterSettings
            }
        }
        val clusterState: ClusterState = mock {
            on { this.metadata } doReturn metadata
        }
        return mock {
            on { state() } doReturn clusterState
        }
    }
}
