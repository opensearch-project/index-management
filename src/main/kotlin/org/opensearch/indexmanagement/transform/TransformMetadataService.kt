/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.DocWriteResponse
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
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.transform.exceptions.TransformMetadataException
import org.opensearch.indexmanagement.transform.model.ContinuousTransformStats
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.model.TransformStats
import org.opensearch.indexmanagement.util.IndexUtils.Companion.hashToFixedSize
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

@SuppressWarnings("ReturnCount")
class TransformMetadataService(private val client: Client, val xContentRegistry: NamedXContentRegistry) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("BlockingMethodInNonBlockingContext")
    suspend fun getMetadata(transform: Transform): TransformMetadata {
        return if (transform.metadataId != null) {
            // update metadata
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, transform.metadataId).routing(transform.id)
            val response: GetResponse = client.suspendUntil { get(getRequest, it) }
            val metadataSource = response.sourceAsBytesRef
            val transformMetadata = metadataSource?.let {
                withContext(Dispatchers.IO) {
                    val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, metadataSource, XContentType.JSON)
                    xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, TransformMetadata.Companion::parse)
                }
            }
            // TODO: Should we attempt to create a new document instead if failed to parse, the only reason this can happen is if someone deleted
            //  the metadata doc?
            transformMetadata ?: throw TransformMetadataException("Failed to parse the existing metadata document")
        } else {
            logger.debug("Creating metadata doc as none exists at the moment for transform job [${transform.id}]")
            createMetadata(transform)
        }
    }

    private suspend fun createMetadata(transform: Transform): TransformMetadata {
        // Including timestamp in the metadata id to prevent clashes if the job was deleted but metadata is not deleted, in that case we want to
        // create a clean metadata doc
        val id = hashToFixedSize("TransformMetadata#${transform.id}#${transform.lastUpdateTime}")
        val metadata = TransformMetadata(
            id = id,
            transformId = transform.id,
            lastUpdatedAt = Instant.now(),
            status = TransformMetadata.Status.INIT,
            stats = TransformStats(0, 0, 0, 0, 0),
            continuousStats = if (transform.continuous) ContinuousTransformStats(null, null) else null
        )
        return writeMetadata(metadata)
    }

    @Suppress("BlockingMethodInNonBlockingContext", "ThrowsCount", "ComplexMethod")
    suspend fun writeMetadata(metadata: TransformMetadata, updating: Boolean = false): TransformMetadata {
        val errorMessage = "Failed to ${if (updating) "update" else "create"} metadata doc ${metadata.id} for transform job ${metadata.transformId}"
        try {
            val builder = metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            val indexRequest = IndexRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
                .source(builder)
                .id(metadata.id)
                .routing(metadata.transformId)
            if (updating) {
                indexRequest.setIfSeqNo(metadata.seqNo).setIfPrimaryTerm(metadata.primaryTerm)
            } else {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            }

            val response: IndexResponse = client.suspendUntil { index(indexRequest, it) }
            return when (response.result) {
                DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED -> {
                    metadata.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm)
                }
                else -> {
                    logger.error(errorMessage)
                    throw TransformMetadataException("Failed to write metadata, received ${response.result?.lowercase} status")
                }
            }
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.error(errorMessage, unwrappedException)
            throw TransformMetadataException(errorMessage, unwrappedException)
        } catch (e: Exception) {
            logger.error(errorMessage, e)
            throw TransformMetadataException(errorMessage, e)
        }
    }
}
