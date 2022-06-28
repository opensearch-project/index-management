/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.junit.Before
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.IndexManagementRestTestCase
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import java.io.InputStream
import java.time.Duration
import java.time.Instant
import java.time.Instant.now

abstract class SnapshotManagementRestTestCase : IndexManagementRestTestCase() {

    var timeout: Instant = Instant.ofEpochSecond(20)

    /**
     * For multi node test, if the shard of config index is moving, then the job scheduler
     * could miss the execution after [updateSMPolicyStartTime]
     * Extending this to be more than 1 minute, so even missed at first place, it could still be
     * picked up to run in the next scheduled job.
     */
    @Before
    fun timeoutForMultiNode() {
        if (isMultiNode) timeout = Instant.ofEpochSecond(70)
    }

    protected fun createSMPolicy(
        smPolicy: SMPolicy,
        refresh: Boolean = true,
    ): SMPolicy {
        val response = createSMPolicyJson(smPolicy.toJsonString(), smPolicy.policyName, refresh)
        return parseSMPolicy(response.entity.content)
    }

    protected fun createSMPolicyJson(
        smPolicyString: String,
        smPolicyName: String,
        refresh: Boolean = true,
    ): Response {
        val response = client()
            .makeRequest(
                "POST",
                "${IndexManagementPlugin.SM_POLICIES_URI}/$smPolicyName?refresh=$refresh",
                emptyMap(),
                StringEntity(smPolicyString, APPLICATION_JSON)
            )
        assertEquals("Unable to create a new snapshot management policy", RestStatus.CREATED, response.restStatus())
        return response
    }

    protected fun getSMPolicy(
        smPolicyName: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): SMPolicy {
        val response = client().makeRequest("GET", "${IndexManagementPlugin.SM_POLICIES_URI}/$smPolicyName", null, header)
        assertEquals("Unable to get snapshot management policy $smPolicyName", RestStatus.OK, response.restStatus())
        return parseSMPolicy(response.entity.content)
    }

    protected fun explainSMPolicy(
        smPolicyName: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): Response {
        val response = client().makeRequest("GET", "${IndexManagementPlugin.SM_POLICIES_URI}/$smPolicyName/_explain", null, header)
        assertEquals("Failed to explain snapshot management policy $smPolicyName", RestStatus.OK, response.restStatus())
        return response
    }

    private fun parseSMPolicy(inputStream: InputStream): SMPolicy {
        val parser = createParser(XContentType.JSON.xContent(), inputStream)
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        var seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO
        lateinit var smPolicy: SMPolicy

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                _ID -> id = parser.text()
                _SEQ_NO -> seqNo = parser.longValue()
                _PRIMARY_TERM -> primaryTerm = parser.longValue()
                SMPolicy.SM_TYPE -> smPolicy = SMPolicy.parse(parser, id, seqNo, primaryTerm)
            }
        }
        return smPolicy
    }

    protected fun SMPolicy.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), APPLICATION_JSON)

    protected fun updateSMPolicyStartTime(update: SMPolicy, desiredStartTimeMillis: Long? = null) {
        // Before updating start time of a job always make sure there are no unassigned shards that could cause the config
        // index to move to a new node and negate this forced start
        if (isMultiNode) {
            waitFor {
                try {
                    client().makeRequest("GET", "_cluster/allocation/explain")
                    fail("Expected 400 Bad Request when there are no unassigned shards to explain")
                } catch (e: ResponseException) {
                    assertEquals(RestStatus.BAD_REQUEST, e.response.restStatus())
                }
            }
        }
        val intervalSchedule = (update.jobSchedule as IntervalSchedule)
        val millis = Duration.of(intervalSchedule.interval.toLong(), intervalSchedule.unit).minusSeconds(2).toMillis()
        val startTimeMillis = desiredStartTimeMillis ?: (now().toEpochMilli() - millis)
        val waitForActiveShards = if (isMultiNode) "all" else "1"
        val response = client().makeRequest(
            "POST", "$INDEX_MANAGEMENT_INDEX/_update/${update.id}?wait_for_active_shards=$waitForActiveShards",
            StringEntity(
                "{\"doc\":{\"sm_policy\":{\"schedule\":{\"interval\":{\"start_time\":\"$startTimeMillis\"}}}}}",
                APPLICATION_JSON
            )
        )

        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }

    fun parseExplainResponse(inputStream: InputStream): List<ExplainSMMetadata> {
        val parser = createParser(XContentType.JSON.xContent(), inputStream)
        // val parser = createParser(builder)
        val smMetadata: MutableList<ExplainSMMetadata> = mutableListOf()

        // while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
        //     println("currentToken ${parser.currentToken()} ${parser.currentName()}")
        // }

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser) // policies
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser)
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            smMetadata.add(ExplainSMMetadata.parse(parser))
        }

        logger.info("${now()}: sm metadata $smMetadata")
        return smMetadata
    }

    data class ExplainSMMetadata(
        val name: String,
        val creation: SMMetadata.WorkflowMetadata?,
        val deletion: SMMetadata.WorkflowMetadata?,
        val policySeqNo: Long?,
        val policyPrimaryTerm: Long?,
        val enabled: Boolean,
    ) {
        companion object {
            fun parse(xcp: XContentParser): ExplainSMMetadata {
                var name: String? = null
                var policySeqNo: Long? = null
                var policyPrimaryTerm: Long? = null
                var creation: SMMetadata.WorkflowMetadata? = null
                var deletion: SMMetadata.WorkflowMetadata? = null
                var enabled: Boolean? = null

                ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        SMPolicy.NAME_FIELD -> name = requireNotNull(xcp.text()) { "The name field of SMPolicy must not be null." }
                        SMMetadata.POLICY_SEQ_NO_FIELD -> policySeqNo = xcp.longValue()
                        SMMetadata.POLICY_PRIMARY_TERM_FIELD -> policyPrimaryTerm = xcp.longValue()
                        SMMetadata.CREATION_FIELD -> creation = SMMetadata.WorkflowMetadata.parse(xcp)
                        SMMetadata.DELETION_FIELD -> deletion = SMMetadata.WorkflowMetadata.parse(xcp)
                        "enabled" -> enabled = xcp.booleanValue()
                    }
                }

                return ExplainSMMetadata(
                    name = requireNotNull(name),
                    policySeqNo = policySeqNo,
                    policyPrimaryTerm = policyPrimaryTerm,
                    creation = creation,
                    deletion = deletion,
                    enabled = requireNotNull(enabled),
                )
            }
        }
    }

    protected fun createRepository(
        repository: String
    ) {
        val response = client()
            .makeRequest(
                "PUT",
                "_snapshot/$repository",
                emptyMap(),
                StringEntity("{\"type\":\"fs\", \"settings\": {\"location\": \"$repository\"}}", APPLICATION_JSON)
            )
        assertEquals("Unable to create a new repository", RestStatus.OK, response.restStatus())
    }
}
