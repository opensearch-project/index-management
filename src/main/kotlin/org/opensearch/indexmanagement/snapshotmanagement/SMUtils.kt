/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.client.Client
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_DOC_ID_SUFFIX
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_METADATA_ID_SUFFIX
import org.opensearch.rest.RestStatus

private val log = LogManager.getLogger("Snapshot Management Helper")

fun smPolicyNameToDocId(policyName: String) = "$policyName$SM_DOC_ID_SUFFIX"
fun smDocIdToPolicyName(id: String) = id.substringBeforeLast(SM_DOC_ID_SUFFIX)
fun getSMMetadataDocId(policyName: String) = "$policyName$SM_METADATA_ID_SUFFIX"
fun smMetadataDocIdToPolicyName(id: String) = id.substringBeforeLast(SM_METADATA_ID_SUFFIX)

@Suppress("RethrowCaughtException", "ThrowsCount")
suspend fun Client.getSMPolicy(policyID: String): SMPolicy {
    try {
        val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, policyID)
        val getResponse: GetResponse = this.suspendUntil { get(getRequest, it) }
        if (!getResponse.isExists || getResponse.isSourceEmpty) {
            throw OpenSearchStatusException("Snapshot management policy not found", RestStatus.NOT_FOUND)
        }
        return parseSMPolicy(getResponse)
    } catch (e: OpenSearchStatusException) {
        throw e
    } catch (e: IndexNotFoundException) {
        throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
    } catch (e: IllegalArgumentException) {
        log.error("Failed to retrieve snapshot management policy [$policyID]", e)
        throw OpenSearchStatusException("Snapshot management policy could not be parsed", RestStatus.INTERNAL_SERVER_ERROR)
    } catch (e: Exception) {
        log.error("Failed to retrieve snapshot management policy [$policyID]", e)
        throw OpenSearchStatusException("Failed to retrieve Snapshot management policy.", RestStatus.NOT_FOUND)
    }
}

@Suppress("RethrowCaughtException", "ThrowsCount")
suspend fun Client.getSMMetadata(metadataID: String): SMMetadata {
    try {
        val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, metadataID)
        val getResponse: GetResponse = this.suspendUntil { get(getRequest, it) }
        if (!getResponse.isExists || getResponse.isSourceEmpty) {
            throw OpenSearchStatusException("Snapshot management metadata not found", RestStatus.NOT_FOUND)
        }
        return parseSMMetadata(getResponse)
    } catch (e: OpenSearchStatusException) {
        throw e
    } catch (e: IndexNotFoundException) {
        throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
    } catch (e: IllegalArgumentException) {
        log.error("Failed to retrieve snapshot management metadata [$metadataID]", e)
        throw OpenSearchStatusException("Snapshot management metadata could not be parsed", RestStatus.INTERNAL_SERVER_ERROR)
    } catch (e: Exception) {
        log.error("Failed to retrieve snapshot management metadata [$metadataID]", e)
        throw OpenSearchStatusException("Failed to retrieve Snapshot management metadata.", RestStatus.NOT_FOUND)
    }
}

fun parseSMPolicy(response: GetResponse, xContentRegistry: NamedXContentRegistry = NamedXContentRegistry.EMPTY): SMPolicy {
    val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.sourceAsBytesRef, XContentType.JSON)
    return xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, SMPolicy.Companion::parse)
}

fun parseSMMetadata(response: GetResponse, xContentRegistry: NamedXContentRegistry = NamedXContentRegistry.EMPTY): SMMetadata {
    val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.sourceAsBytesRef, XContentType.JSON)
    return xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, SMMetadata.Companion::parse)
}

/**
 * Save snapshot management job run metadata
 *
 * @param id: snapshot management job doc id
 */
suspend fun Client.indexMetadata(
    metadata: SMMetadata,
    id: String,
    seqNo: Long = metadata.seqNo,
    primaryTerm: Long = metadata.primaryTerm,
    create: Boolean = false
): IndexResponse {
    val indexReq = IndexRequest(INDEX_MANAGEMENT_INDEX).create(create)
        .id(getSMMetadataDocId(smDocIdToPolicyName(id)))
        .source(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
        .setIfSeqNo(seqNo)
        .setIfPrimaryTerm(primaryTerm)
        .routing(id)

    return suspendUntil { index(indexReq, it) }
}

/**
 * Retrieve snapshot management job run metadata
 *
 * @return null indicate the retrieved metadata doesn't exist
 */
suspend fun Client.getMetadata(job: SMPolicy): SMMetadata? {
    val getReq = GetRequest(INDEX_MANAGEMENT_INDEX, getSMMetadataDocId(smDocIdToPolicyName(job.id))).routing(job.id)
    val getRes: GetResponse = suspendUntil { get(getReq, it) }
    if (getRes.isExists) {
        log.info("sm dev: Get metadata response: ${getRes.sourceAsBytesRef.utf8ToString()}")
        val xcp = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            getRes.sourceAsBytesRef, XContentType.JSON
        )
        val metadata = xcp.parseWithType(getRes.id, getRes.seqNo, getRes.primaryTerm, SMMetadata.Companion::parse)
        log.info("sm dev: Parse metadata: $metadata")
        return metadata
    }
    return null
}
