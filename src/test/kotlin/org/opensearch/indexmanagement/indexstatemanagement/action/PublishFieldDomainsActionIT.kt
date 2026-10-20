/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Request
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.index.fielddomain.DateRangeFieldDomain
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.fielddomain.AttemptPublishFieldDomainsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.readonly.SetReadOnlyStep
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.LockModel
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class PublishFieldDomainsActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test published date and date nanos domains prune without exposing metadata`() {
        val indexName = "${testIndexName}_index"
        val controlIndex = "${testIndexName}_control"
        val policyID = "${testIndexName}_policy"
        val millisField = "event.millis"
        val nanosField = "event.nanos"
        val millisMin = "2024-05-01T00:00:00.000Z"
        val millisMax = "2024-05-03T12:34:56.789Z"
        val nanosMin = "2024-05-01T00:00:00.000000001Z"
        val nanosMax = "2024-05-01T00:00:00.000000009Z"

        val actionConfig = PublishFieldDomainsAction(
            fields = listOf(
                FieldDomainConfig(millisField, DateRangeFieldDomain.TYPE),
                FieldDomainConfig(nanosField, DateRangeFieldDomain.TYPE),
            ),
            index = 1,
        )
        val states = listOf(
            State(
                name = "PublishFieldDomainsState",
                actions = listOf(ReadOnlyAction(0), actionConfig),
                transitions = listOf(),
            ),
        )
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, replicas = "0", mapping = mapping(millisField, nanosField))
        // An index without domains prevents the all-pruned fallback from hiding missing publication.
        createIndex(controlIndex, policyID = null, replicas = "0", mapping = mapping(millisField, nanosField))
        indexDoc(indexName, source = source(millisMin, nanosMax))
        indexDoc(indexName, source = source(millisMax, nanosMin))
        indexDoc(indexName, source = source("2024-05-02T00:00:00.000Z", "2024-05-01T00:00:00.000000005Z"))
        refresh(indexName)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID)
            assertJobLockReleased(managedIndexConfig.id)
        }

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals("true", getIndexBlocksWriteSetting(indexName))
            assertEquals(SetReadOnlyStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message"))
            assertJobLockReleased(managedIndexConfig.id)
        }

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val metadata = getExplainManagedIndexMetaData(indexName)
            assertEquals(AttemptPublishFieldDomainsStep.getSuccessMessage(indexName, 2), metadata.info?.get("message"))
        }

        val metadata = client().makeRequest("GET", "/_cluster/state/metadata/$indexName").asMap()
            .stringMap("metadata")?.stringMap("indices")?.stringMap(indexName)
        assertNotNull("Ordinary index metadata remains visible", metadata)
        assertFalse("Field domains must not be exposed through the API", metadata!!.containsKey(IndexFieldDomainMetadata.CUSTOM_KEY))
        val originalSettings = client().makeRequest("GET", "/_cluster/settings?flat_settings=true").asMap()
            .stringMap("persistent").orEmpty()
        try {
            updateClusterSetting(PRUNING_MIN_SHARDS, "1", false)
            updateClusterSetting(PRUNING_FIELDS, "[\"$millisField\",\"$nanosField\"]", false)
            for (enabled in listOf(false, true)) {
                updateClusterSetting(PRUNING_ENABLED, enabled.toString(), false)
                waitFor {
                    assertBoundarySearches("$indexName,$controlIndex", millisField, millisMin, millisMax, enabled)
                    assertBoundarySearches("$indexName,$controlIndex", nanosField, nanosMin, nanosMax, enabled)
                }
            }
        } finally {
            val restored = listOf(PRUNING_ENABLED, PRUNING_MIN_SHARDS, PRUNING_FIELDS).associateWith { originalSettings[it] }
            val request = Request("PUT", "/_cluster/settings")
            request.setJsonEntity(
                XContentFactory.jsonBuilder().use { builder ->
                    builder.startObject().field("persistent", restored).endObject()
                    BytesReference.bytes(builder).utf8ToString()
                },
            )
            client().performRequest(request)
        }
    }

    private fun mapping(millisField: String, nanosField: String): String =
        """
        "properties": {
          "${millisField.substringBefore(".")}": {
            "properties": {
              "${millisField.substringAfter(".")}": { "type": "date" },
              "${nanosField.substringAfter(".")}": { "type": "date_nanos" }
            }
          }
        }
        """.trimIndent()

    private fun source(millisValue: String, nanosValue: String): String =
        """
        {
          "event": {
            "millis": "$millisValue",
            "nanos": "$nanosValue"
          }
        }
        """.trimIndent()

    private fun assertBoundarySearches(
        indices: String,
        field: String,
        min: String,
        max: String,
        pruning: Boolean,
    ) {
        val ranges = listOf(
            Triple("""{"gte":"$min","lte":"$min"}""", 1L, 0),
            Triple("""{"gte":"$max","lte":"$max"}""", 1L, 0),
            Triple("""{"gt":"$min","lt":"$max"}""", 1L, 0),
            Triple("""{"lt":"$min"}""", 0L, 1),
            Triple("""{"gt":"$max"}""", 0L, 1),
        )
        for ((range, hits, prunableShards) in ranges) {
            val request = Request("POST", "/$indices/_search")
            request.addParameter("request_cache", "false")
            request.addParameter("allow_partial_search_results", "false")
            // Disable shard prefiltering so skipped shards prove that field domains are used.
            request.addParameter("pre_filter_shard_size", Int.MAX_VALUE.toString())
            request.setJsonEntity("""{"size":0,"track_total_hits":true,"query":{"range":{"$field":$range}}}""")
            val response = client().performRequest(request).asMap()
            assertEquals(false, response["timed_out"])
            assertEquals(2, response.stringMap("_shards")?.get("total"))
            assertEquals(0, response.stringMap("_shards")?.get("failed"))
            assertEquals("Unexpected hits for $field: $range", hits, (response.stringMap("hits")!!.stringMap("total")!!["value"] as Number).toLong())
            assertEquals(if (pruning) prunableShards else 0, response.stringMap("_shards")?.get("skipped"))
        }
    }

    private fun assertJobLockReleased(jobId: String) {
        val lockId = LockModel.generateLockId(INDEX_MANAGEMENT_INDEX, jobId)
        val lock = adminClient().makeRequest("GET", "/.opendistro-job-scheduler-lock/_doc/$lockId").asMap()
        assertEquals(true, lock.stringMap("_source")?.get(LockModel.RELEASED))
    }

    companion object {
        private const val PRUNING_ENABLED = "search.index_pruning.enabled"
        private const val PRUNING_MIN_SHARDS = "search.index_pruning.min_shards"
        private const val PRUNING_FIELDS = "search.index_pruning.fields"
    }
}
