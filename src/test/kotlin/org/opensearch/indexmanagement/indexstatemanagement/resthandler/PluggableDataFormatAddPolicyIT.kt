/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILED_INDICES
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILURES
import org.opensearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class PluggableDataFormatAddPolicyIT : IndexStateManagementRestTestCase() {

    fun `test add rollup policy to pluggable dataformat index fails fast`() {
        val indexName = "pluggable-policy-rollup-test"
        val policyId = "rollup-policy-pluggable"

        val settings = Settings.builder().put("index.pluggable.dataformat.enabled", true).build()
        createIndex(indexName, settings, """"properties": {"timestamp": {"type": "date"}, "value": {"type": "long"}}""")

        val policy = """
        {
          "policy": {
            "description": "rollup policy",
            "default_state": "rollup_state",
            "states": [{
              "name": "rollup_state",
              "actions": [{"rollup": {"ism_rollup": {
                "description": "test rollup",
                "target_index": "rollup-target",
                "page_size": 1,
                "dimensions": [{"date_histogram": {"source_field": "timestamp", "fixed_interval": "1h", "timezone": "UTC"}}],
                "metrics": [{"source_field": "value", "metrics": [{"sum": {}}]}]
              }}}],
              "transitions": []
            }]
          }
        }
        """.trimIndent()
        client().makeRequest("PUT", "/_plugins/_ism/policies/$policyId", emptyMap(), StringEntity(policy, ContentType.APPLICATION_JSON))

        val response = client().makeRequest(
            "POST", "/_plugins/_ism/add/$indexName", emptyMap(),
            StringEntity("""{"policy_id": "$policyId"}""", ContentType.APPLICATION_JSON),
        )
        val responseBody = response.asMap()
        assertEquals("Expected 0 updated indices", 0, responseBody[UPDATED_INDICES])
        assertEquals("Expected failures", true, responseBody[FAILURES])
        val failedIndices = responseBody[FAILED_INDICES] as List<Map<String, Any>>
        assertTrue(
            "Should contain failure for pluggable index",
            failedIndices.any {
                (it["index_name"] as String) == indexName &&
                    (it["reason"] as String).contains("Optimized Engine")
            },
        )
    }

    fun `test add transform policy to pluggable dataformat index fails fast`() {
        val indexName = "pluggable-policy-transform-test"
        val policyId = "transform-policy-pluggable"

        val settings = Settings.builder().put("index.pluggable.dataformat.enabled", true).build()
        createIndex(indexName, settings, """"properties": {"timestamp": {"type": "date"}, "value": {"type": "long"}}""")

        val policy = """
        {
          "policy": {
            "description": "transform policy",
            "default_state": "transform_state",
            "states": [{
              "name": "transform_state",
              "actions": [{"transform": {"ism_transform": {
                "description": "test transform",
                "target_index": "transform-target",
                "page_size": 1,
                "data_selection_query": {"match_all": {}},
                "groups": [{"date_histogram": {"source_field": "timestamp", "fixed_interval": "1h", "timezone": "UTC"}}],
                "aggregations": {"sum_value": {"sum": {"field": "value"}}}
              }}}],
              "transitions": []
            }]
          }
        }
        """.trimIndent()
        client().makeRequest("PUT", "/_plugins/_ism/policies/$policyId", emptyMap(), StringEntity(policy, ContentType.APPLICATION_JSON))

        val response = client().makeRequest(
            "POST", "/_plugins/_ism/add/$indexName", emptyMap(),
            StringEntity("""{"policy_id": "$policyId"}""", ContentType.APPLICATION_JSON),
        )
        val responseBody = response.asMap()
        assertEquals("Expected 0 updated indices", 0, responseBody[UPDATED_INDICES])
        assertEquals("Expected failures", true, responseBody[FAILURES])
        val failedIndices = responseBody[FAILED_INDICES] as List<Map<String, Any>>
        assertTrue(
            "Should contain failure for pluggable index",
            failedIndices.any {
                (it["index_name"] as String) == indexName &&
                    (it["reason"] as String).contains("Optimized Engine")
            },
        )
    }

    fun `test add rollup policy to normal index succeeds`() {
        val indexName = "normal-policy-test"
        val policyId = "rollup-policy-normal"

        createIndex(indexName, Settings.EMPTY, """"properties": {"timestamp": {"type": "date"}, "value": {"type": "long"}}""")

        val policy = """
        {
          "policy": {
            "description": "rollup policy",
            "default_state": "rollup_state",
            "states": [{
              "name": "rollup_state",
              "actions": [{"rollup": {"ism_rollup": {
                "description": "test rollup",
                "target_index": "rollup-target-normal",
                "page_size": 1,
                "dimensions": [{"date_histogram": {"source_field": "timestamp", "fixed_interval": "1h", "timezone": "UTC"}}],
                "metrics": [{"source_field": "value", "metrics": [{"sum": {}}]}]
              }}}],
              "transitions": []
            }]
          }
        }
        """.trimIndent()
        client().makeRequest("PUT", "/_plugins/_ism/policies/$policyId", emptyMap(), StringEntity(policy, ContentType.APPLICATION_JSON))

        val response = client().makeRequest(
            "POST", "/_plugins/_ism/add/$indexName", emptyMap(),
            StringEntity("""{"policy_id": "$policyId"}""", ContentType.APPLICATION_JSON),
        )
        val responseBody = response.asMap()
        assertEquals("Expected 1 updated index", 1, responseBody[UPDATED_INDICES])
        assertEquals("Expected no failures", false, responseBody[FAILURES])
    }
}
