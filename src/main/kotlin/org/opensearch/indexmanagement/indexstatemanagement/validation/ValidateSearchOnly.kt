/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.monitor.jvm.JvmService

@OpenForTesting
class ValidateSearchOnly(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService,
) : Validate(settings, clusterService, jvmService) {
    private val logger = LogManager.getLogger(javaClass)

    @Suppress("ReturnSuppressCount", "ReturnCount")
    override fun execute(indexName: String): Validate {
        val indexMetadata = clusterService.state().metadata.index(indexName)

        if (indexMetadata == null) {
            val message = getNoIndexMessage(indexName)
            logger.warn(message)
            validationMessage = message
            validationStatus = ValidationStatus.FAILED
            return this
        }

        if (isAlreadySearchOnly(indexMetadata, indexName)) {
            validationStatus = ValidationStatus.FAILED
            return this
        }

        if (!hasRemoteStorePrerequisites(indexMetadata, indexName)) {
            validationStatus = ValidationStatus.FAILED
            return this
        }

        validationMessage = getValidationPassedMessage(indexName)
        return this
    }

    private fun isAlreadySearchOnly(indexMetadata: IndexMetadata, indexName: String): Boolean {
        val isSearchOnly = indexMetadata.settings.getAsBoolean(INDEX_BLOCKS_SEARCH_ONLY_SETTING, false)
        if (isSearchOnly) {
            val message = getAlreadySearchOnlyMessage(indexName)
            logger.warn(message)
            validationMessage = message
            return true
        }
        return false
    }

    @Suppress("ReturnCount")
    private fun hasRemoteStorePrerequisites(indexMetadata: IndexMetadata, indexName: String): Boolean {
        val clusterSettings = clusterService.state().metadata.settings()
        val isRemoteStoreEnabled = clusterSettings.getAsBoolean("cluster.remote_store.enabled", false)

        if (!isRemoteStoreEnabled) {
            val message = getRemoteStoreDisabledMessage(indexName)
            logger.warn(message)
            validationMessage = message
            return false
        }

        val replicationType = indexMetadata.settings.get("index.replication.type", "DOCUMENT")
        if (replicationType != "SEGMENT") {
            val message = getSegmentReplicationDisabledMessage(indexName)
            logger.warn(message)
            validationMessage = message
            return false
        }

        val searchReplicas = indexMetadata.settings.getAsInt("index.number_of_search_only_replicas", 0)
        if (searchReplicas == 0) {
            val message = getNoSearchReplicasMessage(indexName)
            logger.warn(message)
            validationMessage = message
            return false
        }

        return true
    }

    @Suppress("TooManyFunctions")
    companion object {
        const val name = "validate_search_only"
        private const val INDEX_BLOCKS_SEARCH_ONLY_SETTING = "index.blocks.search_only"

        fun getNoIndexMessage(index: String) = "No such index [index=$index] for search_only action."

        fun getAlreadySearchOnlyMessage(index: String) = "Index [index=$index] is already in search-only mode."

        fun getRemoteStoreDisabledMessage(index: String) = "Cannot scale to zero without remote store enabled [index=$index]"

        fun getSegmentReplicationDisabledMessage(index: String) = "Cannot scale to zero without segment replication [index=$index]"

        fun getNoSearchReplicasMessage(index: String) = "Cannot scale to zero without search replicas [index=$index]"

        fun getValidationPassedMessage(index: String) = "search_only action validation passed for [index=$index]"
    }
}
