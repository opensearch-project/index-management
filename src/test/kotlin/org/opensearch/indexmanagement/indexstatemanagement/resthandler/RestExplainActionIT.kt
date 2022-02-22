/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.util.TOTAL_MANAGED_INDICES
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.util.Locale

class RestExplainActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test single index`() {
        val indexName = "${testIndexName}_movies"
        createIndex(indexName, null)
        val expected = mapOf(
            TOTAL_MANAGED_INDICES to 0,
            indexName to mapOf<String, Any?>(
                explainResponseOpendistroPolicyIdSetting to null,
                explainResponseOpenSearchPolicyIdSetting to null,
                ManagedIndexMetaData.ENABLED to null
            )
        )
        assertResponseMap(expected, getExplainMap(indexName))
    }

    fun `test single index explain all`() {
        val indexName = "${testIndexName}_movies"
        createIndex(indexName, null)
        val expected = mapOf(
            "total_managed_indices" to 0
        )
        assertResponseMap(expected, getExplainMap(null))
    }

    fun `test two indices, one managed one not managed`() {
        // explicitly asks for un-managed index, will return policy_id as null
        val indexName1 = "${testIndexName}_managed"
        val indexName2 = "${testIndexName}_not_managed"
        val policy = createRandomPolicy()
        createIndex(indexName1, policy.id)
        createIndex(indexName2, null)

        val expected = mapOf(
            TOTAL_MANAGED_INDICES to 1,
            indexName1 to mapOf<String, Any>(
                explainResponseOpendistroPolicyIdSetting to policy.id,
                explainResponseOpenSearchPolicyIdSetting to policy.id,
                "index" to indexName1,
                "index_uuid" to getUuid(indexName1),
                "policy_id" to policy.id,
                ManagedIndexMetaData.ENABLED to true
            ),
            indexName2 to mapOf<String, Any?>(
                explainResponseOpendistroPolicyIdSetting to null,
                explainResponseOpenSearchPolicyIdSetting to null,
                ManagedIndexMetaData.ENABLED to null
            )
        )
        waitFor {
            assertResponseMap(expected, getExplainMap("$indexName1,$indexName2"))
        }
    }

    fun `test two indices, one managed one not managed explain all`() {
        // explain all returns only managed indices
        val indexName1 = "${testIndexName}_managed"
        val indexName2 = "${testIndexName}_not_managed"
        val policy = createRandomPolicy()
        createIndex(indexName1, policy.id)
        createIndex(indexName2, null)

        val expected = mapOf(
            indexName1 to mapOf<String, Any>(
                explainResponseOpendistroPolicyIdSetting to policy.id,
                explainResponseOpenSearchPolicyIdSetting to policy.id,
                "index" to indexName1,
                "index_uuid" to getUuid(indexName1),
                "policy_id" to policy.id,
                "enabled" to true
            ),
            "total_managed_indices" to 1
        )
        waitFor {
            assertResponseMap(expected, getExplainMap(null))
        }
    }

    fun `test index pattern`() {
        val indexName1 = "${testIndexName}_pattern"
        val indexName2 = "${indexName1}_2"
        val indexName3 = "${indexName1}_3"
        val policy = createRandomPolicy()
        createIndex(indexName1, policyID = policy.id)
        createIndex(indexName2, policyID = policy.id)
        createIndex(indexName3, null)
        val expected = mapOf(
            TOTAL_MANAGED_INDICES to 2,
            indexName1 to mapOf<String, Any>(
                explainResponseOpendistroPolicyIdSetting to policy.id,
                explainResponseOpenSearchPolicyIdSetting to policy.id,
                "index" to indexName1,
                "index_uuid" to getUuid(indexName1),
                "policy_id" to policy.id,
                ManagedIndexMetaData.ENABLED to true
            ),
            indexName2 to mapOf<String, Any>(
                explainResponseOpendistroPolicyIdSetting to policy.id,
                explainResponseOpenSearchPolicyIdSetting to policy.id,
                "index" to indexName2,
                "index_uuid" to getUuid(indexName2),
                "policy_id" to policy.id,
                ManagedIndexMetaData.ENABLED to true
            ),
            indexName3 to mapOf<String, Any?>(
                explainResponseOpendistroPolicyIdSetting to null,
                explainResponseOpenSearchPolicyIdSetting to null,
                ManagedIndexMetaData.ENABLED to null
            )
        )
        waitFor {
            assertResponseMap(expected, getExplainMap("$indexName1*"))
        }
    }

    fun `test attached policy`() {
        val indexName = "${testIndexName}_watermelon"
        val policy = createRandomPolicy()
        createIndex(indexName, policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val expectedInfoString = mapOf("message" to "Successfully initialized policy: ${policy.id}").toString()
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        explainResponseOpendistroPolicyIdSetting to policy.id::equals,
                        explainResponseOpenSearchPolicyIdSetting to policy.id::equals,
                        ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                        ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                        ManagedIndexMetaData.POLICY_ID to managedIndexConfig.policyID::equals,
                        ManagedIndexMetaData.POLICY_SEQ_NO to policy.seqNo.toInt()::equals,
                        ManagedIndexMetaData.POLICY_PRIMARY_TERM to policy.primaryTerm.toInt()::equals,
                        StateMetaData.STATE to fun(stateMetaDataMap: Any?): Boolean =
                            assertStateEquals(
                                StateMetaData(policy.defaultState, Instant.now().toEpochMilli()),
                                stateMetaDataMap
                            ),
                        PolicyRetryInfoMetaData.RETRY_INFO to fun(retryInfoMetaDataMap: Any?): Boolean =
                            assertRetryInfoEquals(PolicyRetryInfoMetaData(false, 0), retryInfoMetaDataMap),
                        ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString(),
                        ManagedIndexMetaData.ENABLED to true::equals
                    )
                ),
                getExplainMap(indexName)
            )
        }
    }

    fun `test failed policy`() {
        val indexName = "${testIndexName}_melon"
        val policy = createRandomPolicy()
        createIndex(indexName, policy.id)
        val newPolicy = createRandomPolicy()
        val changePolicy = ChangePolicy(newPolicy.id, null, emptyList(), false)
        client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/$indexName", emptyMap(), changePolicy.toHttpEntity()
        )
        val deletePolicyResponse = client().makeRequest(
            RestRequest.Method.DELETE.toString(),
            "${IndexManagementPlugin.LEGACY_POLICY_BASE_URI}/${changePolicy.policyID}"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, deletePolicyResponse.restStatus())

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val expectedInfoString = mapOf("message" to "Fail to load policy: ${changePolicy.policyID}").toString()
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        explainResponseOpendistroPolicyIdSetting to policy.id::equals,
                        explainResponseOpenSearchPolicyIdSetting to policy.id::equals,
                        ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                        ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                        ManagedIndexMetaData.POLICY_ID to newPolicy.id::equals,
                        PolicyRetryInfoMetaData.RETRY_INFO to fun(retryInfoMetaDataMap: Any?): Boolean =
                            assertRetryInfoEquals(PolicyRetryInfoMetaData(true, 0), retryInfoMetaDataMap),
                        ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString(),
                        ManagedIndexMetaData.ENABLED to true::equals
                    )
                ),
                getExplainMap(indexName)
            )
        }
    }

    @Suppress("UNCHECKED_CAST") // Do assertion of the response map here so we don't have many places to do suppression.
    private fun assertResponseMap(expected: Map<String, Any>, actual: Map<String, Any>) {
        assertEquals("Explain Map does not match", expected.size, actual.size)
        for (metaDataEntry in expected) {
            if (metaDataEntry.key == "total_managed_indices") {
                assertEquals(metaDataEntry.value, actual[metaDataEntry.key])
                continue
            }
            val value = metaDataEntry.value as Map<String, Any?>
            actual as Map<String, Map<String, String?>>
            assertMetaDataEntries(value, actual[metaDataEntry.key]!!)
        }
    }

    private fun assertMetaDataEntries(expected: Map<String, Any?>, actual: Map<String, Any?>) {
        assertEquals("MetaDataSize are not the same", expected.size, actual.size)
        for (entry in expected) {
            assertEquals("Expected and actual values does not match", entry.value, actual[entry.key])
        }
    }
}
