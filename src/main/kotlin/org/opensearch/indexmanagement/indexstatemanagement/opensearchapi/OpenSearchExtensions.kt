/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:Suppress("TooManyFunctions")

package org.opensearch.indexmanagement.indexstatemanagement.opensearchapi

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.NoShardAvailableActionException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.Index
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.settings.LegacyOpenDistroManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.opensearchapi.contentParser
import org.opensearch.indexmanagement.opensearchapi.suspendUntil

private val log = LogManager.getLogger("Index Management Helper")

/**
 * Returns the current rollover_alias if it exists otherwise returns null.
 */
fun IndexMetadata.getRolloverAlias(): String? {
    if (this.settings.get(ManagedIndexSettings.ROLLOVER_ALIAS.key).isNullOrBlank()) {
        return if (this.settings.get(LegacyOpenDistroManagedIndexSettings.ROLLOVER_ALIAS.key).isNullOrBlank()) {
            null
        } else {
            this.settings.get(LegacyOpenDistroManagedIndexSettings.ROLLOVER_ALIAS.key)
        }
    }

    return this.settings.get(ManagedIndexSettings.ROLLOVER_ALIAS.key)
}

fun IndexMetadata.getRolloverSkip(): Boolean {
    return this.settings.getAsBoolean(ManagedIndexSettings.ROLLOVER_SKIP.key, false)
}

fun IndexMetadata.getManagedIndexMetadata(): ManagedIndexMetaData? {
    val existingMetaDataMap = this.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA_TYPE)

    if (existingMetaDataMap != null) {
        return ManagedIndexMetaData.fromMap(existingMetaDataMap)
    }
    return null
}

fun getUuidsForClosedIndices(state: ClusterState): MutableList<String> {
    val indexMetadatas = state.metadata.indices
    val closeList = mutableListOf<String>()
    indexMetadatas.forEach {
        // it.key is index name
        if (it.value.state == IndexMetadata.State.CLOSE) {
            closeList.add(it.value.indexUUID)
        }
    }
    return closeList
}

@Suppress("UNCHECKED_CAST")
fun <K, V> Map<K, V?>.filterNotNullValues(): Map<K, V> =
    filterValues { it != null } as Map<K, V>

// get metadata from config index using doc id
@Suppress("ReturnCount")
suspend fun IndexMetadata.getManagedIndexMetadata(client: Client): ManagedIndexMetaData? {
    try {
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(indexUUID))
            .routing(this.indexUUID)
        val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
        if (!getResponse.isExists || getResponse.isSourceEmpty) {
            return null
        }

        return withContext(Dispatchers.IO) {
            val xcp = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef, XContentType.JSON
            )
            ManagedIndexMetaData.parseWithType(xcp, getResponse.id, getResponse.seqNo, getResponse.primaryTerm)
        }
    } catch (e: Exception) {
        when (e) {
            is IndexNotFoundException, is NoShardAvailableActionException -> {
                log.error("Failed to get metadata because no index or shard not available")
            }
            else -> log.error("Failed to get metadata", e)
        }

        return null
    }
}

/**
 * multi-get metadata for indices
 *
 * @return list of metadata
 */
suspend fun Client.mgetManagedIndexMetadata(indices: List<Index>): List<Pair<ManagedIndexMetaData?, Exception?>?> {
    log.debug("trying to get back metadata for indices ${indices.map { it.name }}")

    if (indices.isEmpty()) return emptyList()

    val mgetRequest = MultiGetRequest()
    indices.forEach {
        mgetRequest.add(
            MultiGetRequest.Item(
                INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(it.uuid)
            ).routing(it.uuid)
        )
    }
    var mgetMetadataList = listOf<Pair<ManagedIndexMetaData?, Exception?>?>()
    try {
        val response: MultiGetResponse = this.suspendUntil { multiGet(mgetRequest, it) }
        mgetMetadataList = mgetResponseToList(response)
    } catch (e: ActionRequestValidationException) {
        log.info("No managed index metadata for indices [$indices], ${e.message}")
    } catch (e: Exception) {
        log.error("Failed to multi-get managed index metadata for indices [$indices]", e)
    }
    return mgetMetadataList
}

/**
 * transform multi-get response to list for ManagedIndexMetaData
 *
 * when this function used in change and retry API, if exception is
 * not null, the API will abort and show get metadata failed
 *
 * @return list of Pair of metadata or exception
 */
fun mgetResponseToList(mgetResponse: MultiGetResponse):
    List<Pair<ManagedIndexMetaData?, Exception?>?> {
    val mgetList = mutableListOf<Pair<ManagedIndexMetaData?, Exception?>?>()
    mgetResponse.responses.forEach {
        if (it.isFailed) {
            mgetList.add(Pair(null, it.failure.failure))
        } else if (it.response != null && !it.response.isSourceEmpty) {
            val xcp = contentParser(it.response.sourceAsBytesRef)
            mgetList.add(
                Pair(
                    ManagedIndexMetaData.parseWithType(
                        xcp, it.response.id, it.response.seqNo, it.response.primaryTerm
                    ),
                    null
                )
            )
        } else {
            mgetList.add(null)
        }
    }

    return mgetList
}

fun buildMgetMetadataRequest(clusterState: ClusterState): MultiGetRequest {
    val mgetMetadataRequest = MultiGetRequest()
    clusterState.metadata.indices.map { it.value.index }.forEach {
        mgetMetadataRequest.add(
            MultiGetRequest.Item(
                INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(it.uuid)
            ).routing(it.uuid)
        )
    }
    return mgetMetadataRequest
}

// forIndex means saving to config index, distinguish from Explain and History,
// which only show meaningful partial metadata
@Suppress("ReturnCount")
fun XContentBuilder.addObject(name: String, metadata: ToXContentFragment?, params: ToXContent.Params, forIndex: Boolean = false): XContentBuilder {
    if (metadata != null) return this.buildMetadata(name, metadata, params)
    return if (forIndex) nullField(name) else this
}

fun XContentBuilder.buildMetadata(name: String, metadata: ToXContentFragment, params: ToXContent.Params): XContentBuilder {
    this.startObject(name)
    metadata.toXContent(this, params)
    this.endObject()
    return this
}
