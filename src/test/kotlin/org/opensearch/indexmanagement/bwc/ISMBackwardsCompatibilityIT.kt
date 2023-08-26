/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.bwc

import org.junit.Assert
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.IndexManagementIndices.Companion.HISTORY_WRITE_INDEX_ALIAS
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import org.opensearch.indexmanagement.waitFor
import java.util.Locale

class ISMBackwardsCompatibilityIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

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

    companion object {
        private val CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"))
        private val CLUSTER_NAME = System.getProperty("tests.clustername")
    }

    override fun preserveIndicesUponCompletion(): Boolean = true

    override fun preserveReposUponCompletion(): Boolean = true

    override fun preserveTemplatesUponCompletion(): Boolean = true

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
    fun `test rollover policy backwards compatibility`() {
        val indexNameBase = "${testIndexName}_index"
        val index1 = "$indexNameBase-1"
        val newIndex1 = "$indexNameBase-000002"
        val aliasName1 = "${testIndexName}_alias"

        val index2 = "$indexNameBase-2-1"
        val newIndex2 = "$indexNameBase-2-000002"
        val aliasName2 = "${testIndexName}_alias2"

        val policyID = "${testIndexName}_testPolicyName_doc_1"

        val uri = getPluginUri()
        val responseMap = getAsMap(uri)["nodes"] as Map<String, Map<String, Any>>
        for (response in responseMap.values) {
            val plugins = response["plugins"] as List<Map<String, Any>>
            val pluginNames = plugins.map { plugin -> plugin ["name"] }.toSet()
            when (CLUSTER_TYPE) {
                ClusterType.OLD -> {
                    assertTrue(pluginNames.contains("opendistro-index-management") || pluginNames.contains("opensearch-index-management"))

                    createRolloverPolicy(policyID)

                    createIndex(index1, policyID, aliasName1)
                    createIndex(index2, policyID, aliasName2)

                    // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
                    updateManagedIndexConfigStartTime(getExistingManagedIndexConfig(index1))
                    waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }

                    // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
                    updateManagedIndexConfigStartTime(getExistingManagedIndexConfig(index2))
                    waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index2).policyID) }

                    verifyPendingRollover(index1)
                    verifyPendingRollover(index2)
                }
                ClusterType.MIXED -> {
                    assertTrue(pluginNames.contains("opensearch-index-management"))

                    verifyPendingRollover(index1)
                    verifyPendingRollover(index2)
                }
                ClusterType.UPGRADED -> {
                    assertTrue(pluginNames.contains("opensearch-index-management"))

                    verifyPendingRollover(index1)
                    insertSampleData(index = index1, docCount = 5, delay = 0)
                    verifySuccessfulRollover(index1, newIndex1)

                    verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, configSchemaVersion)
                    verifyIndexSchemaVersion(HISTORY_WRITE_INDEX_ALIAS, historySchemaVersion)

                    insertSampleData(index = index2, docCount = 5, delay = 0)
                    verifySuccessfulRollover(index2, newIndex2)

                    deleteIndex("$indexNameBase*")
                }
            }
            break
        }
    }

    private fun createRolloverPolicy(policyID: String) {
        val policy = """
            {
              "policy": {
                "policy_id": "$policyID",
                "description": "description",
                "default_state": "RolloverAction",
                "states": [
                  {
                    "name": "RolloverAction",
                    "actions": [
                      {
                        "rollover": {
                          "min_doc_count": 3,
                          "min_index_age": "2d"
                        }
                      }
                    ],
                    "transitions": [
                      
                    ]
                  }
                ]
              }
            }
        """.trimIndent()
        createPolicyJson(policy, policyID)
    }

    @Suppress("UNCHECKED_CAST")
    private fun verifyPendingRollover(index: String) {
        val managedIndexConfig = getExistingManagedIndexConfig(index)
        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(index).info as Map<String, Any?>
            assertEquals(
                "Index rollover before it met the condition.",
                AttemptRolloverStep.getPendingMessage(index), info["message"]
            )
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverAction.MIN_INDEX_AGE_FIELD, RolloverAction.MIN_DOC_COUNT_FIELD), conditions.keys
            )
            val minAge = conditions[RolloverAction.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverAction.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min age condition", "2d", minAge["condition"])
            assertTrue("Did not have min age current", minAge["current"] is String)
            assertEquals("Did not have min doc count condition", 3, minDocCount["condition"])
            assertEquals("Did not have min doc count current", 0, minDocCount["current"])
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun verifySuccessfulRollover(index: String, newIndex: String) {
        val managedIndexConfig = getExistingManagedIndexConfig(index)
        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val metadata = getExplainManagedIndexMetaData(index)
            val info = metadata.info as Map<String, Any?>
            assertEquals("Index did not rollover", AttemptRolloverStep.getSuccessMessage(index), info["message"])
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverAction.MIN_INDEX_AGE_FIELD, RolloverAction.MIN_DOC_COUNT_FIELD), conditions.keys
            )
            val minAge = conditions[RolloverAction.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverAction.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min age condition", "2d", minAge["condition"])
            assertTrue("Did not have min age current", minAge["current"] is String)
            assertEquals("Did not have min doc count condition", 3, minDocCount["condition"])
            assertEquals("Did not have min doc count current", 5, minDocCount["current"])
            assertEquals("Did not have rolled over index name", metadata.rolledOverIndexName, newIndex)
        }
        Assert.assertTrue("New rollover index does not exist.", indexExists(newIndex))
    }
}
