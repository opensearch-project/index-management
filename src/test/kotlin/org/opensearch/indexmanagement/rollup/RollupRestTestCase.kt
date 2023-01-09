/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup

import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.opensearch.client.Request
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.IndexManagementRestTestCase
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.settings.RollupSettings
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import java.time.Duration
import java.time.Instant

abstract class RollupRestTestCase : IndexManagementRestTestCase() {

    companion object {
        @AfterClass
        @JvmStatic fun clearIndicesAfterClass() {
            wipeAllIndices()
        }
    }

    @After
    @Suppress("UNCHECKED_CAST")
    fun KillAllCancallableRunningTasks() {
        client().makeRequest("POST", "_tasks/_cancel?actions=*")
        waitFor {
            val response = client().makeRequest("GET", "_tasks")
            val nodes = response.asMap()["nodes"] as Map<String, Any?>
            var hasCancallableRunningTasks = false
            nodes.forEach {
                val tasks = (it.value as Map<String, Any?>)["tasks"] as Map<String, Any?>
                tasks.forEach { e ->
                    if ((e.value as Map<String, Any?>)["cancellable"] as Boolean) {
                        hasCancallableRunningTasks = true
                    }
                }
            }
            assertFalse(hasCancallableRunningTasks)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun waitForCancallableTasksToFinish() {
        waitFor {
            val response = client().makeRequest("GET", "_tasks")
            val nodes = response.asMap()["nodes"] as Map<String, Any?>
            var hasCancallableRunningTasks = false
            nodes.forEach {
                val tasks = (it.value as Map<String, Any?>)["tasks"] as Map<String, Any?>
                tasks.forEach { e ->
                    if ((e.value as Map<String, Any?>)["cancellable"] as Boolean) {
                        hasCancallableRunningTasks = true
                        logger.info("cancellable task running: ${e.key}")
                    }
                }
            }
            assertFalse(hasCancallableRunningTasks)
        }
    }

    @Before
    fun setDebugLogLevel() {
        client().makeRequest(
            "PUT", "_cluster/settings",
            StringEntity(
                """
                {
                    "transient": {
                        "logger.org.opensearch.indexmanagement.rollup":"DEBUG",
                        "logger.org.opensearch.jobscheduler":"DEBUG"
                    }
                }
                """.trimIndent(),
                APPLICATION_JSON
            )
        )
    }

    protected fun createRollup(
        rollup: Rollup,
        rollupId: String = OpenSearchTestCase.randomAlphaOfLength(10),
        refresh: Boolean = true,
        client: RestClient? = null
    ): Rollup {
        val response = createRollupJson(rollup.toJsonString(), rollupId, refresh, client)

        val rollupJson = JsonXContent.jsonXContent
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.entity.content)
            .map()
        val createdId = rollupJson["_id"] as String
        assertEquals("Rollup ids are not the same", rollupId, createdId)
        return rollup.copy(
            id = createdId,
            seqNo = (rollupJson["_seq_no"] as Int).toLong(),
            primaryTerm = (rollupJson["_primary_term"] as Int).toLong()
        )
    }

    protected fun createRollupJson(
        rollupString: String,
        rollupId: String,
        refresh: Boolean = true,
        userClient: RestClient? = null
    ): Response {
        val client = userClient ?: client()
        val response = client
            .makeRequest(
                "PUT",
                "$ROLLUP_JOBS_BASE_URI/$rollupId?refresh=$refresh",
                emptyMap(),
                StringEntity(rollupString, APPLICATION_JSON)
            )
        assertEquals("Unable to create a new rollup", RestStatus.CREATED, response.restStatus())
        return response
    }

    protected fun createRandomRollup(refresh: Boolean = true): Rollup {
        val rollup = randomRollup()
        val rollupId = createRollup(rollup, refresh = refresh).id
        return getRollup(rollupId = rollupId)
    }

    // TODO: Maybe clean-up and use XContentFactory.jsonBuilder() to create mappings json
    protected fun createRollupMappingString(rollup: Rollup): String {
        var mappingString = ""
        var addCommaPrefix = false
        rollup.dimensions.forEach {
            val fieldType = when (it.type) {
                Dimension.Type.DATE_HISTOGRAM -> "date"
                Dimension.Type.HISTOGRAM -> "long"
                Dimension.Type.TERMS -> "keyword"
            }
            val string = "${if (addCommaPrefix) "," else ""}\"${it.sourceField}\":{\"type\": \"$fieldType\"}"
            addCommaPrefix = true
            mappingString += string
        }
        rollup.metrics.forEach {
            val string = "${if (addCommaPrefix) "," else ""}\"${it.sourceField}\":{\"type\": \"long\"}"
            addCommaPrefix = true
            mappingString += string
        }
        mappingString = "\"properties\":{$mappingString}"
        return mappingString
    }

    protected fun createRollupSourceIndex(rollup: Rollup, settings: Settings = Settings.EMPTY) {
        createIndex(rollup.sourceIndex, settings, createRollupMappingString(rollup))
    }

    protected fun putDateDocumentInSourceIndex(rollup: Rollup) {
        val dateHistogram = rollup.dimensions.first()
        val request = """
            {
              "${dateHistogram.sourceField}" : "${Instant.now()}"
            }
        """.trimIndent()
        val response = client().makeRequest(
            "POST", "${rollup.sourceIndex}/_doc?refresh=true",
            emptyMap(), StringEntity(request, APPLICATION_JSON)
        )
        assertEquals("Request failed", RestStatus.CREATED, response.restStatus())
    }

    protected fun getRollup(
        rollupId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): Rollup {
        val response = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/$rollupId", null, header)
        assertEquals("Unable to get rollup $rollupId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        var seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO
        lateinit var rollup: Rollup

        while (parser.nextToken() != Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                _ID -> id = parser.text()
                _SEQ_NO -> seqNo = parser.longValue()
                _PRIMARY_TERM -> primaryTerm = parser.longValue()
                Rollup.ROLLUP_TYPE -> rollup = Rollup.parse(parser, id, seqNo, primaryTerm)
            }
        }
        return rollup
    }

    protected fun getRollupMetadata(
        metadataId: String,
        refresh: Boolean = true,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): RollupMetadata {
        val response = client().makeRequest("GET", "$INDEX_MANAGEMENT_INDEX/_doc/$metadataId?refresh=$refresh", null, header)
        assertEquals("Unable to get rollup metadata $metadataId", RestStatus.OK, response.restStatus())
        return parseRollupMetadata(response)
    }

    protected fun getRollupMetadataWithRoutingId(
        routingId: String,
        metadataId: String,
        refresh: Boolean = true,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): RollupMetadata {
        val response = client().makeRequest("GET", "$INDEX_MANAGEMENT_INDEX/_doc/$metadataId?routing=$routingId&refresh=$refresh", null, header)
        assertEquals("Unable to get rollup metadata $metadataId", RestStatus.OK, response.restStatus())

        return parseRollupMetadata(response)
    }

    private fun parseRollupMetadata(response: Response): RollupMetadata {
        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        var seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO
        lateinit var metadata: RollupMetadata

        while (parser.nextToken() != Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                _ID -> id = parser.text()
                _SEQ_NO -> seqNo = parser.longValue()
                _PRIMARY_TERM -> primaryTerm = parser.longValue()
                RollupMetadata.ROLLUP_METADATA_TYPE -> metadata = RollupMetadata.parse(parser, id, seqNo, primaryTerm)
            }
        }

        return metadata
    }

    protected fun Rollup.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), APPLICATION_JSON)

    protected fun updateRollupStartTime(update: Rollup, desiredStartTimeMillis: Long? = null) {
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
        // TODO flaky: Add this log to confirm this update is missed by job scheduler
        // This miss is because shard remove, job scheduler deschedule on the original node and reschedule on another node
        // However the shard comes back, and job scheduler deschedule on the another node and reschedule on the original node
        // During this period, this update got missed
        // Since from the log, this happens very fast (within 0.1~0.2s), the above cluster explain may not have the granularity to catch this.
        logger.info("Update rollup start time to $startTimeMillis")
        val response = client().makeRequest(
            "POST", "$INDEX_MANAGEMENT_INDEX/_update/${update.id}?wait_for_active_shards=$waitForActiveShards&refresh=true",
            StringEntity(
                "{\"doc\":{\"rollup\":{\"schedule\":{\"interval\":{\"start_time\":" +
                    "\"$startTimeMillis\"}}}}}",
                APPLICATION_JSON
            )
        )

        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }

    protected fun updateSearchAllJobsClusterSetting(value: Boolean) {
        val formattedValue = "\"${value}\""
        val request = """
            {
                "persistent": {
                    "${RollupSettings.ROLLUP_SEARCH_ALL_JOBS.key}": $formattedValue
                }
            }
        """.trimIndent()
        val res = client().makeRequest(
            "PUT", "_cluster/settings", emptyMap(),
            StringEntity(request, APPLICATION_JSON)
        )
        assertEquals("Request failed", RestStatus.OK, res.restStatus())
    }

    protected fun createSampleIndexForQSQTest(index: String) {
        val mapping = """
            "properties": {
                "event_ts": {
                    "type": "date"
                },
                "test": {
                    "properties": {
                        "fff": {
                            "type": "keyword"
                        },
                        "vvv": {
                            "type": "keyword"
                        }
                    }
                },
                "state": {
                    "type": "keyword"
                },
                "state_ext": {
                    "type": "keyword"
                },
                "state_ext2": {
                    "type": "keyword"
                },
                "state_ordinal": {
                    "type": "long"
                },
                "abc test": {
                    "type": "long"
                },
                "earnings": {
                    "type": "long"
                }
                        
            }
        """.trimIndent()
        createIndex(index, Settings.EMPTY, mapping)

        for (i in 1..5) {
            val doc = """
                {
                    "event_ts": "2019-01-01T12:10:30Z",
                    "test.fff": "12345",
                    "test.vvv": "54321",
                    "state": "TX",
                    "state_ext": "CA",
                    "state_ext2": "TX",
                    "abc test": 123,
                    "state_ordinal": ${i % 3},
                    "earnings": $i
                }
            """.trimIndent()
            indexDoc(index, "$i", doc)
        }
        for (i in 6..8) {
            val doc = """
                {
                    "event_ts": "2019-01-01T12:10:30Z",
                    "state": "TA",
                    "state_ext": "SE",
                    "state_ext2": "CA",
                    "state_ordinal": ${i % 3},   
                    "abc test": 123,
                    "earnings": $i
                }
            """.trimIndent()
            indexDoc(index, "$i", doc)
        }
        for (i in 9..11) {
            val doc = """
                {
                    "event_ts": "2019-01-02T12:10:30Z",
                    "state": "CA",
                    "state_ext": "MA",
                    "state_ext2": "CA",
                    "state_ordinal": ${i % 3},       
                    "abc test": 123,                                 
                    "earnings": $i
                }
            """.trimIndent()
            indexDoc(index, "$i", doc)
        }
    }

    protected fun indexDoc(index: String, id: String, doc: String) {
        val request = Request("POST", "$index/_doc/$id/?refresh=true")
        request.setJsonEntity(doc)
        val resp = client().performRequest(request)
        assertEquals(HttpStatus.SC_CREATED, resp.restStatus().status)
    }
}
