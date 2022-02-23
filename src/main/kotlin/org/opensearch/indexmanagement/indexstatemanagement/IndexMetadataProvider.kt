/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.spi.indexstatemanagement.IndexMetadataService
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ISMIndexMetadata

/**
 * Consolidates IndexMetadataServices from all extensions to delegate the index metadata provider based on index type.
 * Provides index metadata of specified index names using the implementation denoted by the index type.
 */
class IndexMetadataProvider(
    val settings: Settings,
    val client: Client,
    val clusterService: ClusterService,
    val services: MutableMap<String, IndexMetadataService>,
) {

    @Volatile private var restrictedIndexPattern = ManagedIndexSettings.RESTRICTED_INDEX_PATTERN.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN) {
            restrictedIndexPattern = it
        }
    }

    fun isUnManageableIndex(index: String): Boolean {
        return Regex(restrictedIndexPattern).matches(index)
    }

    suspend fun getISMIndexMetadata(type: String = DEFAULT_INDEX_TYPE, indexNames: List<String>): Map<String, ISMIndexMetadata> {
        val service = services[type] ?: throw IllegalArgumentException(getTypeNotRecognizedMessage(type))
        return service.getMetadata(indexNames, client, clusterService)
    }

    fun addMetadataServices(newServices: Map<String, IndexMetadataService>) {
        val duplicateIndexType = newServices.keys.firstOrNull { services.containsKey(it) }
        if (duplicateIndexType != null) {
            throw IllegalArgumentException(getDuplicateServicesMessage(duplicateIndexType))
        }
        services.putAll(newServices)
    }

    suspend fun getAllISMIndexMetadata(): Set<ISMIndexMetadata> {
        val metadata = mutableSetOf<ISMIndexMetadata>()
        services.forEach { (_, service) ->
            val serviceMetadata = service.getMetadataForAllIndices(client, clusterService)
            metadata.addAll(serviceMetadata.values)
        }

        return metadata
    }

    companion object {
        const val EVALUATION_FAILURE_MESSAGE = "Matches restricted index pattern defined in the cluster setting"
        fun getTypeNotRecognizedMessage(indexType: String) = "Index type [type=$indexType] was not recognized when trying to get index metadata"
        fun getDuplicateServicesMessage(indexType: String) = "Multiple metadata services attempted to assign a service to the index type [$indexType]"
    }
}
