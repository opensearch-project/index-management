/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.bwc

import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.opensearch.common.settings.Settings
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_POLICY_BASE_URI
import org.opensearch.indexmanagement.IndexManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder

class IndexManagementBackwardsCompatibilityIT : IndexManagementRestTestCase() {

    companion object {
        private val CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"))
        private val CLUSTER_NAME = System.getProperty("tests.clustername")
    }

    override fun preserveIndicesUponCompletion(): Boolean = true

    override fun preserveReposUponCompletion(): Boolean = true

    override fun preserveTemplatesUponCompletion(): Boolean = true

    override fun preserveODFEIndicesAfterTest(): Boolean = true

    override fun restClientSettings(): Settings {
        return Settings.builder()
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(CLIENT_SOCKET_TIMEOUT, "90s")
            .build()
    }

    @Throws(Exception::class)
    @Suppress("UNCHECKED_CAST")
    fun `test policy backwards compatibility`() {
        val uri = getPluginUri()
        val responseMap = getAsMap(uri)["nodes"] as Map<String, Map<String, Any>>
        for (response in responseMap.values) {
            val plugins = response["plugins"] as List<Map<String, Any>>
            val pluginNames = plugins.map { plugin -> plugin ["name"] }.toSet()
            when (CLUSTER_TYPE) {
                ClusterType.OLD -> {
                    assertTrue(pluginNames.contains("opendistro-index-management"))
                    createBasicPolicy()
                }
                ClusterType.MIXED -> {
                    assertTrue(pluginNames.contains("opensearch-index-management"))
                    verifyPolicyExists(LEGACY_POLICY_BASE_URI)
                }
                ClusterType.UPGRADED -> {
                    assertTrue(pluginNames.contains("opensearch-index-management"))
                    verifyPolicyExists(LEGACY_POLICY_BASE_URI)
                }
            }
            break
        }
    }

    private enum class ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        companion object {
            fun parse(value: String): ClusterType {
                return when (value) {
                    "old_cluster" -> OLD
                    "mixed_cluster" -> MIXED
                    "upgraded_cluster" -> UPGRADED
                    else -> throw AssertionError("Unknown cluster type: $value")
                }
            }
        }
    }

    private fun getPluginUri(): String {
        return when (CLUSTER_TYPE) {
            ClusterType.OLD -> "_nodes/$CLUSTER_NAME-0/plugins"
            ClusterType.MIXED -> {
                when (System.getProperty("tests.rest.bwcsuite_round")) {
                    "second" -> "_nodes/$CLUSTER_NAME-1/plugins"
                    "third" -> "_nodes/$CLUSTER_NAME-2/plugins"
                    else -> "_nodes/$CLUSTER_NAME-0/plugins"
                }
            }
            ClusterType.UPGRADED -> "_nodes/plugins"
        }
    }

    @Throws(Exception::class)
    private fun createBasicPolicy() {
        val indexName = "test_bwc_index"
        val legacyPolicyString = """
            {
              "policy": {
                "description": "bwc_test_policy",
                "default_state": "ingest",
                "states": [
                  {
                    "name": "ingest",
                    "actions": [
                      {
                        "rollover": {
                          "min_doc_count": 5
                        }
                      }
                    ],
                    "transitions": [
                      {
                        "state_name": "search"
                      }
                    ]
                  },
                  {
                    "name": "search",
                    "actions": [],
                    "transitions": [
                      {
                        "state_name": "delete",
                        "conditions": {
                          "min_index_age": "5m"
                        }
                      }
                    ]
                  },
                  {
                    "name": "delete",
                    "actions": [
                      {
                        "delete": {}
                      }
                    ],
                    "transitions": []
                  }
                ]
              }
            }
        """.trimIndent()
        createIndex(indexName, Settings.EMPTY)

        val createResponse = client().makeRequest(
            method = "PUT",
            endpoint = "$LEGACY_POLICY_BASE_URI/bwc_test_policy?refresh=true",
            params = emptyMap(),
            entity = StringEntity(legacyPolicyString, APPLICATION_JSON)
        )

        assertEquals("Create policy failed", RestStatus.CREATED, createResponse.restStatus())
        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int
        assertNotEquals("Create policy response is missing id", Policy.NO_ID, createdId)
        assertTrue("Create policy response has incorrect version", createdVersion > 0)
    }

    @Throws(Exception::class)
    @Suppress("UNCHECKED_CAST")
    private fun verifyPolicyExists(uri: String) {
        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val getResponse = client().makeRequest(
            "GET",
            "$uri/bwc_test_policy",
            emptyMap(),
            StringEntity(search, APPLICATION_JSON)
        )
        assertEquals("Get policy failed", RestStatus.OK, getResponse.restStatus())
    }
}
