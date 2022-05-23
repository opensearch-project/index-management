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
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.DefaultIndexMetadataService
import org.opensearch.indexmanagement.indexstatemanagement.settings.LegacyOpenDistroManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.opensearchapi.contentParser
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import java.time.Instant

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
    if (this.settings.get(ManagedIndexSettings.ROLLOVER_SKIP.key).isNullOrBlank()) {
        return this.settings.getAsBoolean(LegacyOpenDistroManagedIndexSettings.ROLLOVER_SKIP.key, false)
    }
    return this.settings.getAsBoolean(ManagedIndexSettings.ROLLOVER_SKIP.key, false)
}

fun IndexMetadata.getManagedIndexMetadata(): ManagedIndexMetaData? {
    val existingMetaDataMap = this.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA_TYPE)

    if (existingMetaDataMap != null) {
        return ManagedIndexMetaData.fromMap(existingMetaDataMap)
    }
    return null
}

fun getUuidsForClosedIndices(state: ClusterState, defaultIndexMetadataService: DefaultIndexMetadataService): MutableList<String> {
    val indexMetadatas = state.metadata.indices
    val closeList = mutableListOf<String>()
    indexMetadatas.forEach {
        // it.key is index name
        if (it.value.state == IndexMetadata.State.CLOSE) {
            closeList.add(defaultIndexMetadataService.getCustomIndexUUID(it.value))
        }
    }
    return closeList
}

@Suppress("UNCHECKED_CAST")
fun <K, V> Map<K, V?>.filterNotNullValues(): Map<K, V> =
    filterValues { it != null } as Map<K, V>

/**
 * Get metadata from config index
 *
 * @return metadata object and get call successful or not
 */
@Suppress("ReturnCount")
suspend fun Client.getManagedIndexMetadata(indexUUID: String): Pair<ManagedIndexMetaData?, Boolean> {
    try {
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(indexUUID))
            .routing(indexUUID)
        val getResponse: GetResponse = this.suspendUntil { get(getRequest, it) }
        if (!getResponse.isExists || getResponse.isSourceEmpty) {
            return Pair(null, true)
        }

        val metadata = withContext(Dispatchers.IO) {
            val xcp = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef, XContentType.JSON
            )
            ManagedIndexMetaData.parseWithType(xcp, getResponse.id, getResponse.seqNo, getResponse.primaryTerm)
        }
        return Pair(metadata, true)
    } catch (e: Exception) {
        when (e) {
            is IndexNotFoundException, is NoShardAvailableActionException -> {
                log.error("Failed to get metadata because no index or shard not available")
            }
            else -> log.error("Failed to get metadata", e)
        }

        return Pair(null, false)
    }
}

/**
 * multi-get metadata for indices
 *
 * @return list of metadata
 */
suspend fun Client.mgetManagedIndexMetadata(indexUuids: List<String>): List<Pair<ManagedIndexMetaData?, Exception?>?> {
    log.debug("trying to get back metadata for index [$indexUuids]")
    if (indexUuids.isEmpty()) return emptyList()

    val mgetRequest = MultiGetRequest()
    indexUuids.forEach {
        mgetRequest.add(
            MultiGetRequest.Item(
                INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(it)
            ).routing(it)
        )
    }
    var mgetMetadataList = listOf<Pair<ManagedIndexMetaData?, Exception?>?>()
    try {
        val response: MultiGetResponse = this.suspendUntil { multiGet(mgetRequest, it) }
        mgetMetadataList = mgetResponseToMap(response).map { it.value }
    } catch (e: ActionRequestValidationException) {
        log.info("No managed index metadata for indices [$indexUuids], ${e.message}")
    } catch (e: Exception) {
        log.error("Failed to multi-get managed index metadata for indices [$indexUuids]", e)
    }

    return mgetMetadataList
}

/**
 * transform multi-get response to list for ManagedIndexMetaData
 *
 * when this function used in change and retry API, if exception is
 * not null, the API will abort and show get metadata failed
 *
 * @return map of <indexuuid>#metadata to Pair of metadata or exception
 */
fun mgetResponseToMap(mgetResponse: MultiGetResponse): Map<String, Pair<ManagedIndexMetaData?, Exception?>?> {
    val mgetMap = mutableMapOf<String, Pair<ManagedIndexMetaData?, Exception?>?>()
    mgetResponse.responses.forEach {
        if (it.isFailed) {
            mgetMap[it.id] = Pair(null, it.failure.failure)
        } else if (it.response != null && !it.response.isSourceEmpty) {
            val xcp = contentParser(it.response.sourceAsBytesRef)
            mgetMap[it.id] = Pair(ManagedIndexMetaData.parseWithType(xcp, it.response.id, it.response.seqNo, it.response.primaryTerm), null)
        } else {
            mgetMap[it.id] = null
        }
    }

    return mgetMap
}

fun buildMgetMetadataRequest(indexUuids: List<String>): MultiGetRequest {
    val mgetMetadataRequest = MultiGetRequest()
    indexUuids.forEach { mgetMetadataRequest.add(MultiGetRequest.Item(INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(it)).routing(it)) }
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

// Get the oldest rollover time or null if index was never rolled over
fun IndexMetadata.getOldestRolloverTime(): Instant? {
    return this.rolloverInfos.values()
        .map { it.value.time }
        .minOrNull() // oldest should be min as its epoch time
        ?.let { Instant.ofEpochMilli(it) }
}
