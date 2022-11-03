/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.util.SHOW_POLICY_QUERY_PARAM
import org.opensearch.indexmanagement.indexstatemanagement.util.TOTAL_MANAGED_INDICES
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.opensearchapi.toMap
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.util.Locale

class RestExplainActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test single index`() {
        disableValidationService()
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
        disableValidationService()
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
        disableValidationService()
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
        disableValidationService()
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

    fun `test search query string`() {
        disableValidationService()
        val indexName1 = "$testIndexName-search-query-string"
        val indexName2 = "$indexName1-testing-2"
        val indexName3 = "$indexName1-testing-3"
        val indexName4 = "$indexName1-testing-4-2022-02-15"
        val indexName5 = "$indexName1-testing-5-15-02-2022"
        val dataStreamName = "$indexName1-data-stream"
        createDataStream(dataStreamName)

        val policy = createRandomPolicy()
        createIndex(indexName1, policyID = policy.id)
        createIndex(indexName2, policyID = policy.id)
        createIndex(indexName3, null)
        createIndex(indexName4, policyID = policy.id)
        createIndex(indexName5, policyID = policy.id)
        addPolicyToIndex(dataStreamName, policy.id)
        val indexName1Map = indexName1 to mapOf<String, Any>(
            explainResponseOpendistroPolicyIdSetting to policy.id,
            explainResponseOpenSearchPolicyIdSetting to policy.id,
            "index" to indexName1,
            "index_uuid" to getUuid(indexName1),
            "policy_id" to policy.id,
            "enabled" to true
        )
        val indexName2Map = indexName2 to mapOf<String, Any>(
            explainResponseOpendistroPolicyIdSetting to policy.id,
            explainResponseOpenSearchPolicyIdSetting to policy.id,
            "index" to indexName2,
            "index_uuid" to getUuid(indexName2),
            "policy_id" to policy.id,
            "enabled" to true
        )
        val indexName4Map = indexName4 to mapOf<String, Any>(
            explainResponseOpendistroPolicyIdSetting to policy.id,
            explainResponseOpenSearchPolicyIdSetting to policy.id,
            "index" to indexName4,
            "index_uuid" to getUuid(indexName4),
            "policy_id" to policy.id,
            "enabled" to true
        )
        val indexName5Map = indexName5 to mapOf<String, Any>(
            explainResponseOpendistroPolicyIdSetting to policy.id,
            explainResponseOpenSearchPolicyIdSetting to policy.id,
            "index" to indexName5,
            "index_uuid" to getUuid(indexName5),
            "policy_id" to policy.id,
            "enabled" to true
        )
        val datastreamMap = ".ds-$dataStreamName-000001" to mapOf<String, Any>(
            explainResponseOpendistroPolicyIdSetting to policy.id,
            explainResponseOpenSearchPolicyIdSetting to policy.id,
            "index" to ".ds-$dataStreamName-000001",
            "index_uuid" to getUuid(".ds-$dataStreamName-000001"),
            "policy_id" to policy.id,
            "enabled" to true
        )

        waitFor {
            val expected = mapOf(
                indexName1Map,
                indexName2Map,
                indexName4Map,
                indexName5Map,
                "total_managed_indices" to 4
            )
            // These should match all non datastream managed indices
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=$testIndexName*"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=$testIndexName-*"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=$testIndexName-search-*"))
        }

        waitFor {
            val expected = mapOf(
                indexName1Map,
                indexName2Map,
                indexName4Map,
                indexName5Map,
                datastreamMap,
                "total_managed_indices" to 5
            )
            // These should match all managed indices including datastreams
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=*$testIndexName-*"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=*search*"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=*search-query*"))
        }

        waitFor {
            val expected = mapOf(
                datastreamMap,
                "total_managed_indices" to 1
            )
            // These should match all datastream managed indices (and system/hidden indices)
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=.*"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=.ds-$testIndexName-*"))
        }

        waitFor {
            val expected = mapOf(
                indexName4Map,
                "total_managed_indices" to 1
            )
            // These should match all just the single index, and validates that it does not match the 15-02-2022 index
            // i.e. if it was still matching on tokens then ["2022", "02", "15"] would match both which we don't want
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=*2022-02-15"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=*2022-02-15*"))
        }
    }

    fun `test attached policy`() {
        disableValidationService()
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
                        ManagedIndexMetaData.INDEX_CREATION_DATE to fun(indexCreationDate: Any?): Boolean = (indexCreationDate as Long) > 1L,
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
        disableValidationService()
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
                        ManagedIndexMetaData.INDEX_CREATION_DATE to fun(indexCreationDate: Any?): Boolean = (indexCreationDate as Long) > 1L,
                        ManagedIndexMetaData.ENABLED to true::equals
                    )
                ),
                getExplainMap(indexName)
            )
        }
    }

    fun `test show_applied_policy query parameter`() {
        disableValidationService()
        val indexName = "${testIndexName}_show_applied_policy"
        val policy = createRandomPolicy()
        createIndex(indexName, policy.id)

        val expectedPolicy = policy.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE_AND_USER).toMap()
        val expected = mapOf(
            indexName to mapOf<String, Any>(
                explainResponseOpendistroPolicyIdSetting to policy.id,
                explainResponseOpenSearchPolicyIdSetting to policy.id,
                "index" to indexName,
                "index_uuid" to getUuid(indexName),
                "policy_id" to policy.id,
                ManagedIndexMetaData.ENABLED to true,
                "policy" to expectedPolicy,
            ),
            TOTAL_MANAGED_INDICES to 1,
        )
        waitFor {
            logger.info(getExplainMap(indexName, queryParams = SHOW_POLICY_QUERY_PARAM))
            assertResponseMap(expected, getExplainMap(indexName, queryParams = SHOW_POLICY_QUERY_PARAM))
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
