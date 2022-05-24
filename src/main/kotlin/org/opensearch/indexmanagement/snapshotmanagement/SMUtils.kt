/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.client.Client
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin
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
