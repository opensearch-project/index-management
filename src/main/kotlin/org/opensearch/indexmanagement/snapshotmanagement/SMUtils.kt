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

suspend fun getSMPolicy(client: Client, policyID: String): SMPolicy {
    val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, policyID)
    val getResponse: GetResponse = try {
        client.suspendUntil { get(getRequest, it) }
    } catch (e: IndexNotFoundException) {
        throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
    }
    if (!getResponse.isExists) {
        throw OpenSearchStatusException("Snapshot management policy not found", RestStatus.NOT_FOUND)
    }
    val smPolicy = try {
        parseSMPolicy(getResponse)
    } catch (e: IllegalArgumentException) {
        throw OpenSearchStatusException("Snapshot management policy not found", RestStatus.NOT_FOUND)
    }
    return smPolicy
}

suspend fun getSMMetadata(client: Client, metadataID: String): SMMetadata? {
    val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, metadataID)
    val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
    if (!getResponse.isExists) {
        return null
    }
    return try {
        parseSMMetadata(getResponse)
    } catch (e: IllegalArgumentException) {
        null
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