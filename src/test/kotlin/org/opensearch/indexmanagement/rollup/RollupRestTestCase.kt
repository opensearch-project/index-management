/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.HttpEntity
import org.apache.hc.core5.http.HttpHeaders
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.http.message.BasicHeader
import org.junit.After
import org.junit.AfterClass
import org.opensearch.client.Request
import org.opensearch.client.Response
import org.opensearch.client.RestClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
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
import java.time.Instant

abstract class RollupRestTestCase : IndexManagementRestTestCase() {
    @After
    @Suppress("UNCHECKED_CAST")
    fun killAllCancellableRunningTasks() {
        client().makeRequest("POST", "_tasks/_cancel?actions=*")
        waitFor {
            val response = client().makeRequest("GET", "_tasks")
            val nodes = response.asMap()["nodes"] as Map<String, Any?>
            var hasCancellableRunningTasks = false
            nodes.forEach {
                val tasks = (it.value as Map<String, Any?>)["tasks"] as Map<String, Any?>
                tasks.forEach { e ->
                    if ((e.value as Map<String, Any?>)["cancellable"] as Boolean) {
                        hasCancellableRunningTasks = true
                    }
                }
            }
            assertFalse(hasCancellableRunningTasks)
        }
    }

    @Suppress("UNCHECKED_CAST")
    protected fun stopAllRollupJobs(): List<String> {
        val stoppedJobIds = mutableListOf<String>()
        try {
            val response = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI?size=1000")
            val rollupsList = response.asMap()["rollups"] as? List<Map<String, Any?>> ?: emptyList()

            rollupsList.forEach { rollupMap ->
                val rollupObj = rollupMap["rollup"] as? Map<String, Any?> ?: return@forEach
                val id = rollupMap["_id"] as? String ?: return@forEach
                val enabled = rollupObj["enabled"] as? Boolean ?: false

                if (enabled) {
                    try {
                        client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/$id/_stop")
                        stoppedJobIds.add(id)
                        logger.debug("Stopped rollup job during test cleanup: $id")
                    } catch (e: Exception) {
                        logger.debug("Failed to stop rollup $id during cleanup: ${e.message}")
                    }
                }
            }
        } catch (e: Exception) {
            logger.warn("Error stopping rollup jobs during test cleanup", e)
        }
        return stoppedJobIds
    }

    /**
     * Waits for all specified rollup jobs to be disabled.
     *
     * Once a job is disabled (enabled = false), it won't start new executions.
     * Any in-flight execution will complete its current iteration, but since we're
     * wiping all indices anyway, we just need to ensure no new executions will start.
     */
    protected fun waitForRollupJobsToStop(jobIds: List<String>) {
        if (jobIds.isEmpty()) return

        waitFor {
            val allDisabled = jobIds.all { jobId -> isRollupJobDisabled(jobId) }
            assertTrue("Rollup jobs were not disabled within timeout", allDisabled)
        }
    }

    private fun isRollupJobDisabled(jobId: String): Boolean =
        try {
            val rollup = getRollup(jobId)
            if (rollup.enabled) {
                logger.debug("Waiting for rollup job $jobId to be disabled")
                false
            } else {
                true
            }
        } catch (e: Exception) {
            // Job might have been deleted, consider it disabled
            logger.debug("Job $jobId not found: ${e.message}")
            true
        }

    companion object {
        @AfterClass
        @JvmStatic fun clearIndicesAfterClass() {
            wipeAllIndices()
        }
    }

    protected fun createRollup(
        rollup: Rollup,
        rollupId: String,
        refresh: Boolean = true,
        client: RestClient? = null,
    ): Rollup {
        val response = createRollupJson(rollup.toJsonString(), rollupId, refresh, client)

        val rollupJson =
            JsonXContent.jsonXContent
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.entity.content)
                .map()
        val createdId = rollupJson["_id"] as String
        assertEquals("Rollup ids are not the same", rollupId, createdId)
        return rollup.copy(
            id = createdId,
            seqNo = (rollupJson["_seq_no"] as Int).toLong(),
            primaryTerm = (rollupJson["_primary_term"] as Int).toLong(),
        )
    }

    protected fun createRollupJson(
        rollupString: String,
        rollupId: String,
        refresh: Boolean = true,
        userClient: RestClient? = null,
    ): Response {
        val client = userClient ?: client()
        val response =
            client
                .makeRequest(
                    "PUT",
                    "$ROLLUP_JOBS_BASE_URI/$rollupId?refresh=$refresh",
                    emptyMap(),
                    StringEntity(rollupString, ContentType.APPLICATION_JSON),
                )
        assertEquals("Unable to create a new rollup", RestStatus.CREATED, response.restStatus())
        return response
    }

    protected fun createRandomRollup(refresh: Boolean = true): Rollup {
        val rollup = randomRollup()
        val rollupId = createRollup(rollup, rollupId = rollup.id, refresh = refresh).id
        return getRollup(rollupId = rollupId)
    }

    // TODO: can be replaced with createRandomRollup if implement assertEqual for mappings with "dynamic"=true fields
    protected fun createRandomRollupWithoutTargetSettings(refresh: Boolean = true): Rollup {
        val rollup = randomRollup().copy(targetIndexSettings = null)
        val rollupId = createRollup(rollup, rollupId = rollup.id, refresh = refresh).id
        return getRollup(rollupId = rollupId)
    }

    // TODO: Maybe clean-up and use XContentFactory.jsonBuilder() to create mappings json
    protected fun createRollupMappingString(rollup: Rollup): String {
        var mappingString = ""
        var addCommaPrefix = false
        rollup.dimensions.forEach {
            val fieldType =
                when (it.type) {
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
        val request =
            """
            {
              "${dateHistogram.sourceField}" : "${Instant.now()}"
            }
            """.trimIndent()
        val response =
            client().makeRequest(
                "POST", "${rollup.sourceIndex}/_doc?refresh=true",
                emptyMap(), StringEntity(request, ContentType.APPLICATION_JSON),
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
        val response = adminClient().makeRequest("GET", "$INDEX_MANAGEMENT_INDEX/_doc/$metadataId?refresh=$refresh", null, header)
        assertEquals("Unable to get rollup metadata $metadataId", RestStatus.OK, response.restStatus())
        return parseRollupMetadata(response)
    }

    protected fun getRollupMetadataWithRoutingId(
        routingId: String,
        metadataId: String,
        refresh: Boolean = true,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): RollupMetadata {
        val response = adminClient().makeRequest("GET", "$INDEX_MANAGEMENT_INDEX/_doc/$metadataId?routing=$routingId&refresh=$refresh", null, header)
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

    protected fun Rollup.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), ContentType.APPLICATION_JSON)

    protected fun updateSearchAllJobsClusterSetting(value: Boolean) {
        val formattedValue = "\"${value}\""
        val request =
            """
            {
                "persistent": {
                    "${RollupSettings.ROLLUP_SEARCH_ALL_JOBS.key}": $formattedValue
                }
            }
            """.trimIndent()
        val res =
            client().makeRequest(
                "PUT", "_cluster/settings", emptyMap(),
                StringEntity(request, ContentType.APPLICATION_JSON),
            )
        assertEquals("Request failed", RestStatus.OK, res.restStatus())
    }

    protected fun updateSearchRawRollupClusterSetting(value: Boolean) {
        val formattedValue = "\"${value}\""
        val request =
            """
            {
                "persistent": {
                    "${RollupSettings.ROLLUP_SEARCH_SOURCE_INDICES.key}": $formattedValue
                }
            }
            """.trimIndent()
        val res =
            client().makeRequest(
                "PUT", "_cluster/settings", emptyMap(),
                StringEntity(request, ContentType.APPLICATION_JSON),
            )
        assertEquals("Request failed", RestStatus.OK, res.restStatus())
    }

    protected fun createSampleIndexForQSQTest(index: String) {
        val mapping =
            """
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
            val doc =
                """
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
            val doc =
                """
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
            val doc =
                """
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
