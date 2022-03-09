/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.IndexMetadataService
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ISMIndexMetadata

class DefaultIndexMetadataService(val customUUIDSetting: String? = null) : IndexMetadataService {

    /**
     * Returns the default index metadata needed for ISM
     */
    @Suppress("SpreadOperator")
    override suspend fun getMetadata(indices: List<String>, client: Client, clusterService: ClusterService): Map<String, ISMIndexMetadata> {
        val indexNameToMetadata: MutableMap<String, ISMIndexMetadata> = HashMap()

        // We want to go through all cluster indices - open/closed/hidden
        val lenientExpandOptions = IndicesOptions.lenientExpandHidden()
        val clusterStateRequest = ClusterStateRequest()
            .clear()
            .indices(*indices.toTypedArray())
            .metadata(true)
            .local(false)
            .waitForTimeout(TimeValue.timeValueMillis(DEFAULT_GET_METADATA_TIMEOUT_IN_MILLIS))
            .indicesOptions(lenientExpandOptions)

        val response: ClusterStateResponse = client.suspendUntil { client.admin().cluster().state(clusterStateRequest, it) }

        response.state.metadata.indices.forEach {
            // TODO waiting to add document count until it is definitely needed
            val uuid = getCustomIndexUUID(it.value)
            val indexMetadata = ISMIndexMetadata(uuid, it.value.creationDate, -1)
            indexNameToMetadata[it.key] = indexMetadata
        }

        return indexNameToMetadata
    }

    /*
     * If an extension wants Index Management to determine cluster state indices UUID based on a custom index setting if
     * present of cluster state, the extension will override this customUUID setting. This allows an index to migrate off
     * cluster and back while using this persistent uuid.
     */
    fun getCustomIndexUUID(indexMetadata: IndexMetadata): String {
        return if (customUUIDSetting != null) {
            indexMetadata.settings.get(customUUIDSetting, indexMetadata.indexUUID)
        } else {
            indexMetadata.indexUUID
        }
    }

    override suspend fun getMetadataForAllIndices(client: Client, clusterService: ClusterService): Map<String, ISMIndexMetadata> {
        return getMetadata(listOf("*"), client, clusterService)
    }

    companion object {
        const val DEFAULT_GET_METADATA_TIMEOUT_IN_MILLIS = 30000L
    }
}
