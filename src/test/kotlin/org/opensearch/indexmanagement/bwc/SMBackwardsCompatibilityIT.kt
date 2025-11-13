/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.bwc

import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import java.util.Locale

class SMBackwardsCompatibilityIT : SnapshotManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    private enum class ClusterType {
        OLD,
        MIXED,
        UPGRADED,
        ;

        companion object {
            fun parse(value: String): ClusterType = when (value) {
                "old_cluster" -> OLD
                "mixed_cluster" -> MIXED
                "upgraded_cluster" -> UPGRADED
                else -> throw AssertionError("Unknown cluster type: $value")
            }
        }
    }

    private fun getPluginUri(): String = when (CLUSTER_TYPE) {
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

    companion object {
        private val CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"))
        private val CLUSTER_NAME = System.getProperty("tests.clustername")
    }

    override fun preserveIndicesUponCompletion(): Boolean = true

    override fun preserveReposUponCompletion(): Boolean = true

    override fun preserveTemplatesUponCompletion(): Boolean = true

    override fun restClientSettings(): Settings = Settings.builder()
        .put(super.restClientSettings())
        // increase the timeout here to 90 seconds to handle long waits for a green
        // cluster health. the waits for green need to be longer than a minute to
        // account for delayed shards
        .put(CLIENT_SOCKET_TIMEOUT, "90s")
        .build()

    @Throws(Exception::class)
    @Suppress("UNCHECKED_CAST")
    fun `test snapshot management backwards compatibility`() {
        val traditionalPolicyName = "${testIndexName}_traditional"
        val deletionOnlyPolicyName = "${testIndexName}_deletion_only"
        val patternPolicyName = "${testIndexName}_pattern"

        val uri = getPluginUri()
        val responseMap = getAsMap(uri)["nodes"] as Map<String, Map<String, Any>>
        for (response in responseMap.values) {
            val plugins = response["plugins"] as List<Map<String, Any>>
            val pluginNames = plugins.map { plugin -> plugin["name"] }.toSet()
            when (CLUSTER_TYPE) {
                ClusterType.OLD -> {
                    assertTrue(pluginNames.contains("opendistro-index-management") || pluginNames.contains("opensearch-index-management"))

                    createRepository("test-repo")

                    // Create traditional policy with both creation and deletion (pre-3.2.0 format)
                    createTraditionalPolicy(traditionalPolicyName)

                    // Verify traditional policy works
                    val traditionalPolicy = getSMPolicy(traditionalPolicyName)
                    assertNotNull("Traditional policy creation should exist", traditionalPolicy.creation)
                    assertNotNull("Traditional policy deletion should exist", traditionalPolicy.deletion)

                    // Verify explain response works in old version (covers old version serialization path)
                    val explainResponse = explainSMPolicy(traditionalPolicyName)
                    val explainMetadata = parseExplainResponse(explainResponse.entity.content)
                    assertTrue("Explain should return results", explainMetadata.isNotEmpty())
                }
                ClusterType.MIXED -> {
                    assertTrue(pluginNames.contains("opensearch-index-management"))

                    // Verify traditional policy still works during rolling upgrade
                    val traditionalPolicy = getSMPolicy(traditionalPolicyName)
                    assertNotNull("Traditional policy creation should exist", traditionalPolicy.creation)
                    assertNotNull("Traditional policy deletion should exist", traditionalPolicy.deletion)

                    // Verify explain response works during rolling upgrade (mixed old/new version serialization)
                    val explainResponse = explainSMPolicy(traditionalPolicyName)
                    val explainMetadata = parseExplainResponse(explainResponse.entity.content)
                    assertTrue("Explain should return results", explainMetadata.isNotEmpty())
                }
                ClusterType.UPGRADED -> {
                    assertTrue(pluginNames.contains("opensearch-index-management"))

                    // Verify traditional policy still works after full upgrade
                    val traditionalPolicy = getSMPolicy(traditionalPolicyName)
                    assertNotNull("Traditional policy creation should exist", traditionalPolicy.creation)
                    assertNotNull("Traditional policy deletion should exist", traditionalPolicy.deletion)

                    // Verify explain response for traditional policy on upgraded cluster (new version serialization)
                    val traditionalExplainResponse = explainSMPolicy(traditionalPolicyName)
                    val traditionalExplainMetadata = parseExplainResponse(traditionalExplainResponse.entity.content)
                    assertTrue("Explain should return results for traditional policy", traditionalExplainMetadata.isNotEmpty())

                    // Now test new features on upgraded cluster

                    // Create deletion-only policy (3.2.0+ feature)
                    createDeletionOnlyPolicy(deletionOnlyPolicyName)
                    val deletionOnlyPolicy = getSMPolicy(deletionOnlyPolicyName)
                    assertNull("Deletion-only policy creation should be null", deletionOnlyPolicy.creation)
                    assertNotNull("Deletion-only policy deletion should exist", deletionOnlyPolicy.deletion)

                    // Verify explain response for deletion-only policy (tests null creation serialization with optionalField)
                    val deletionOnlyExplainResponse = explainSMPolicy(deletionOnlyPolicyName)
                    val deletionOnlyExplainMetadata = parseExplainResponse(deletionOnlyExplainResponse.entity.content)
                    assertTrue("Explain should return results for deletion-only policy", deletionOnlyExplainMetadata.isNotEmpty())

                    // Create policy with snapshot pattern (3.2.0+ feature)
                    createPatternPolicy(patternPolicyName)
                    val patternPolicy = getSMPolicy(patternPolicyName)
                    assertNotNull("Pattern policy creation should exist", patternPolicy.creation)
                    assertNotNull("Pattern policy deletion should exist", patternPolicy.deletion)
                    assertEquals("Pattern policy should have snapshot pattern", "external-backup-*", patternPolicy.deletion?.snapshotPattern)

                    // Verify explain response for pattern policy
                    val patternExplainResponse = explainSMPolicy(patternPolicyName)
                    val patternExplainMetadata = parseExplainResponse(patternExplainResponse.entity.content)
                    assertTrue("Explain should return results for pattern policy", patternExplainMetadata.isNotEmpty())
                }
            }
            break
        }
    }

    private fun createTraditionalPolicy(policyName: String) {
        val policy = """
            {
              "sm_policy": {
                "name": "$policyName",
                "description": "Traditional SM policy with creation and deletion",
                "creation": {
                  "schedule": {
                    "cron": {
                      "expression": "0 1 * * *",
                      "timezone": "UTC"
                    }
                  }
                },
                "deletion": {
                  "schedule": {
                    "cron": {
                      "expression": "0 2 * * *",
                      "timezone": "UTC"
                    }
                  },
                  "condition": {
                    "max_age": "7d",
                    "min_count": 1
                  }
                },
                "snapshot_config": {
                  "repository": "test-repo"
                }
              }
            }
        """.trimIndent()
        createSMPolicyJson(policy, policyName)
    }

    private fun createDeletionOnlyPolicy(policyName: String) {
        val policy = """
            {
              "sm_policy": {
                "name": "$policyName",
                "description": "Deletion-only SM policy (3.2.0+ feature)",
                "deletion": {
                  "schedule": {
                    "cron": {
                      "expression": "0 2 * * *",
                      "timezone": "UTC"
                    }
                  },
                  "condition": {
                    "max_age": "14d",
                    "min_count": 2
                  }
                },
                "snapshot_config": {
                  "repository": "test-repo"
                }
              }
            }
        """.trimIndent()
        createSMPolicyJson(policy, policyName)
    }

    private fun createPatternPolicy(policyName: String) {
        val policy = """
            {
              "sm_policy": {
                "name": "$policyName",
                "description": "SM policy with snapshot pattern (3.2.0+ feature)",
                "creation": {
                  "schedule": {
                    "cron": {
                      "expression": "0 1 * * *",
                      "timezone": "UTC"
                    }
                  }
                },
                "deletion": {
                  "schedule": {
                    "cron": {
                      "expression": "0 2 * * *",
                      "timezone": "UTC"
                    }
                  },
                  "condition": {
                    "max_age": "30d",
                    "min_count": 3
                  },
                  "snapshot_pattern": "external-backup-*"
                },
                "snapshot_config": {
                  "repository": "test-repo"
                }
              }
            }
        """.trimIndent()
        createSMPolicyJson(policy, policyName)
    }
}
