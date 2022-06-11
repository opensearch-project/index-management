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

abstract class SnapshotManagementRestTestCase : IndexManagementRestTestCase() {

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

    protected fun parseSMPolicy(inputStream: InputStream): SMPolicy {
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
        val startTimeMillis = desiredStartTimeMillis ?: (Instant.now().toEpochMilli() - millis)
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
}
