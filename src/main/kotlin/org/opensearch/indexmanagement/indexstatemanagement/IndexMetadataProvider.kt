/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
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

    suspend fun getISMIndexMetadataByType(type: String = DEFAULT_INDEX_TYPE, indexNames: List<String>): Map<String, ISMIndexMetadata> {
        val service = services[type] ?: throw IllegalArgumentException(getTypeNotRecognizedMessage(type))
        if (type != DEFAULT_INDEX_TYPE && indexNames.size > 1) throw IllegalArgumentException(MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR)
        return service.getMetadata(indexNames, client, clusterService)
    }

    suspend fun getAllISMIndexMetadataByType(type: String = DEFAULT_INDEX_TYPE): Map<String, ISMIndexMetadata> {
        val service = services[type] ?: throw IllegalArgumentException(getTypeNotRecognizedMessage(type))
        return service.getMetadataForAllIndices(client, clusterService)
    }

    /*
     * Attempts to get the index metadata for of all indexNames for each of the index types designated in the types parameter.
     * Returns a map of <index type to <index names to index metadata>>
     */
    suspend fun getMultiTypeISMIndexMetadata(
        types: List<String> = services.keys.toList(),
        indexNames: List<String>
    ): Map<String, Map<String, ISMIndexMetadata>> = coroutineScope {
        if (types.any { it != DEFAULT_INDEX_TYPE } && indexNames.size > 1) throw IllegalArgumentException(MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR)
        val requests = ArrayList<Deferred<Pair<String, Map<String, ISMIndexMetadata>>>>()
        // Start all index metadata requests at the same time
        types.forEach { type ->
            requests.add(async { type to getISMIndexMetadataByType(type, indexNames) })
        }
        // Wait for all index metadata responses, and return
        requests.awaitAll().toMap()
    }

    fun addMetadataServices(newServices: Map<String, IndexMetadataService>) {
        val duplicateIndexType = newServices.keys.firstOrNull { services.containsKey(it) }
        if (duplicateIndexType != null) {
            throw IllegalArgumentException(getDuplicateServicesMessage(duplicateIndexType))
        }
        services.putAll(newServices)
    }

    suspend fun getAllISMIndexMetadata(): Set<ISMIndexMetadata> = coroutineScope {
        val metadata = mutableSetOf<ISMIndexMetadata>()
        val requests = ArrayList<Deferred<Map<String, ISMIndexMetadata>>>()
        services.forEach { (_, service) ->
            requests.add(async { service.getMetadataForAllIndices(client, clusterService) })
        }

        requests.awaitAll().forEach { metadata.addAll(it.values) }

        metadata
    }

    fun getIndexMetadataWriteOverrideSettings(): List<String> {
        return services.values.mapNotNull { it.getIndexMetadataWriteOverrideSetting() }
    }

    companion object {
        const val EVALUATION_FAILURE_MESSAGE = "Matches restricted index pattern defined in the cluster setting"
        const val MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR = "Cannot get metadata for more than one index name/pattern when using a custom index type"
        fun getTypeNotRecognizedMessage(indexType: String) = "Index type [type=$indexType] was not recognized when trying to get index metadata"
        fun getDuplicateServicesMessage(indexType: String) = "Multiple metadata services attempted to assign a service to the index type [$indexType]"
    }
}
