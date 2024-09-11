/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.junit.AfterClass
import org.opensearch.client.Response
import org.opensearch.client.RestClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.IndexManagementRestTestCase
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import org.opensearch.search.SearchModule

abstract class TransformRestTestCase : IndexManagementRestTestCase() {

    companion object {
        @AfterClass
        @JvmStatic fun clearIndicesAfterClass() {
            wipeAllIndices()
        }
    }

    protected fun createTransform(
        transform: Transform,
        transformId: String = randomAlphaOfLength(10),
        refresh: Boolean = true,
        client: RestClient? = null,
    ): Transform {
        if (!indexExists(transform.sourceIndex)) {
            createTransformSourceIndex(transform)
        }
        val response = createTransformJson(transform.toJsonString(), transformId, refresh, client)

        val transformJson = createParser(XContentType.JSON.xContent(), response.entity.content)
            .map()
        val createdId = transformJson["_id"] as String
        assertEquals("Transform ids are not the same", transformId, createdId)
        return transform.copy(
            id = createdId,
            seqNo = (transformJson["_seq_no"] as Int).toLong(),
            primaryTerm = (transformJson["_primary_term"] as Int).toLong(),
        )
    }

    private fun createTransformJson(
        transformString: String,
        transformId: String,
        refresh: Boolean = true,
        userClient: RestClient? = null,
    ): Response {
        val client = userClient ?: client()
        val response = client
            .makeRequest(
                "PUT",
                "$TRANSFORM_BASE_URI/$transformId?refresh=$refresh",
                emptyMap(),
                StringEntity(transformString, APPLICATION_JSON),
            )
        assertEquals("Unable to create a new transform", RestStatus.CREATED, response.restStatus())
        return response
    }

    protected fun disableTransform(transformId: String) {
        val response = client()
            .makeRequest(
                "POST",
                "$TRANSFORM_BASE_URI/$transformId/_stop",
                emptyMap(),
            )
        assertEquals("Unable to disable transform $transformId", RestStatus.OK, response.restStatus())
    }

    protected fun createRandomTransform(refresh: Boolean = true): Transform {
        val transform = randomTransform()
        val transformId = createTransform(transform, refresh = refresh).id
        return getTransform(transformId = transformId)
    }

    protected fun createTransformSourceIndex(transform: Transform, settings: Settings = Settings.EMPTY) {
        var mappingString = ""
        var addCommaPrefix = false
        transform.groups.forEach {
            val fieldType = when (it.type) {
                Dimension.Type.DATE_HISTOGRAM -> "date"
                Dimension.Type.HISTOGRAM -> "long"
                Dimension.Type.TERMS -> "keyword"
            }
            val string = "${if (addCommaPrefix) "," else ""}\"${it.sourceField}\":{\"type\": \"$fieldType\"}"
            addCommaPrefix = true
            mappingString += string
        }
        mappingString = "\"properties\":{$mappingString}"
        createIndex(transform.sourceIndex, settings, mappingString)
    }

    protected fun getTransformMetadata(metadataId: String): TransformMetadata {
        val response =
            adminClient().makeRequest(
                "GET", "$INDEX_MANAGEMENT_INDEX/_doc/$metadataId", null, BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
            )
        assertEquals("Unable to get transform metadata $metadataId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        var seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO
        lateinit var metadata: TransformMetadata

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                _ID -> id = parser.text()
                _SEQ_NO -> seqNo = parser.longValue()
                _PRIMARY_TERM -> primaryTerm = parser.longValue()
                TransformMetadata.TRANSFORM_METADATA_TYPE -> metadata = TransformMetadata.parse(parser, id, seqNo, primaryTerm)
            }
        }

        return metadata
    }

    protected fun getTransform(
        transformId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
        userClient: RestClient? = null,
    ): Transform {
        val client = userClient ?: client()
        val response = client.makeRequest("GET", "$TRANSFORM_BASE_URI/$transformId", null, header)
        assertEquals("Unable to get transform $transformId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        var seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO
        lateinit var transform: Transform

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                _ID -> id = parser.text()
                _SEQ_NO -> seqNo = parser.longValue()
                _PRIMARY_TERM -> primaryTerm = parser.longValue()
                Transform.TRANSFORM_TYPE -> transform = Transform.parse(parser, id, seqNo, primaryTerm)
            }
        }
        return transform
    }

    protected fun getTransformMetadata(
        metadataId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): TransformMetadata {
        val response = client().makeRequest("GET", "$INDEX_MANAGEMENT_INDEX/_doc/$metadataId", null, header)
        assertEquals("Unable to get transform metadata $metadataId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        var seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO
        lateinit var metadata: TransformMetadata

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                _ID -> id = parser.text()
                _SEQ_NO -> seqNo = parser.longValue()
                _PRIMARY_TERM -> primaryTerm = parser.longValue()
                TransformMetadata.TRANSFORM_METADATA_TYPE -> metadata = TransformMetadata.parse(parser, id, seqNo, primaryTerm)
            }
        }

        return metadata
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getTransformDocumentsBehind(
        transformId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): Map<String, Any> {
        val explainResponse = client().makeRequest("GET", "$TRANSFORM_BASE_URI/$transformId/_explain", null, header)
        assertEquals(RestStatus.OK, explainResponse.restStatus())

        val explainResponseMap = explainResponse.asMap()
        val explainMetadata = explainResponseMap[transformId] as Map<String, Any>
        val metadata = explainMetadata["transform_metadata"] as Map<String, Any>
        val continuousStats = metadata["continuous_stats"] as Map<String, Any>
        return continuousStats["documents_behind"] as Map<String, Long>
    }

    protected fun Transform.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), ContentType.APPLICATION_JSON)

    override fun xContentRegistry(): NamedXContentRegistry {
        return NamedXContentRegistry(SearchModule(Settings.EMPTY, emptyList()).namedXContents)
    }
}
