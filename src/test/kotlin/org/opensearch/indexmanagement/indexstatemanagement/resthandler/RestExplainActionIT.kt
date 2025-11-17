/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.OpenAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadOnlyAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.model.ExplainFilter
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.util.SHOW_POLICY_QUERY_PARAM
import org.opensearch.indexmanagement.indexstatemanagement.util.TOTAL_MANAGED_INDICES
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.opensearchapi.toMap
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest
import java.time.Instant
import java.util.Locale

class RestExplainActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test single index`() {
        val indexName = "${testIndexName}_movies"
        createIndex(indexName, null)
        val expected =
            mapOf(
                TOTAL_MANAGED_INDICES to 0,
                indexName to
                    mapOf<String, Any?>(
                        explainResponseOpendistroPolicyIdSetting to null,
                        explainResponseOpenSearchPolicyIdSetting to null,
                        ManagedIndexMetaData.ENABLED to null,
                    ),
            )
        assertResponseMap(expected, getExplainMap(indexName))
    }

    fun `test single index explain all`() {
        val indexName = "${testIndexName}_movies"
        createIndex(indexName, null)
        val expected =
            mapOf(
                "total_managed_indices" to 0,
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

        val expected =
            mapOf(
                TOTAL_MANAGED_INDICES to 1,
                indexName1 to
                    mapOf<String, Any>(
                        explainResponseOpendistroPolicyIdSetting to policy.id,
                        explainResponseOpenSearchPolicyIdSetting to policy.id,
                        "index" to indexName1,
                        "index_uuid" to getUuid(indexName1),
                        "policy_id" to policy.id,
                        ManagedIndexMetaData.ENABLED to true,
                    ),
                indexName2 to
                    mapOf<String, Any?>(
                        explainResponseOpendistroPolicyIdSetting to null,
                        explainResponseOpenSearchPolicyIdSetting to null,
                        ManagedIndexMetaData.ENABLED to null,
                    ),
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

        val expected =
            mapOf(
                indexName1 to
                    mapOf<String, Any>(
                        explainResponseOpendistroPolicyIdSetting to policy.id,
                        explainResponseOpenSearchPolicyIdSetting to policy.id,
                        "index" to indexName1,
                        "index_uuid" to getUuid(indexName1),
                        "policy_id" to policy.id,
                        "enabled" to true,
                    ),
                "total_managed_indices" to 1,
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
        val expected =
            mapOf(
                TOTAL_MANAGED_INDICES to 2,
                indexName1 to
                    mapOf<String, Any>(
                        explainResponseOpendistroPolicyIdSetting to policy.id,
                        explainResponseOpenSearchPolicyIdSetting to policy.id,
                        "index" to indexName1,
                        "index_uuid" to getUuid(indexName1),
                        "policy_id" to policy.id,
                        ManagedIndexMetaData.ENABLED to true,
                    ),
                indexName2 to
                    mapOf<String, Any>(
                        explainResponseOpendistroPolicyIdSetting to policy.id,
                        explainResponseOpenSearchPolicyIdSetting to policy.id,
                        "index" to indexName2,
                        "index_uuid" to getUuid(indexName2),
                        "policy_id" to policy.id,
                        ManagedIndexMetaData.ENABLED to true,
                    ),
                indexName3 to
                    mapOf<String, Any?>(
                        explainResponseOpendistroPolicyIdSetting to null,
                        explainResponseOpenSearchPolicyIdSetting to null,
                        ManagedIndexMetaData.ENABLED to null,
                    ),
            )
        waitFor {
            assertResponseMap(expected, getExplainMap("$indexName1*"))
        }
    }

    fun `test search query string`() {
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
        val indexName1Map =
            indexName1 to
                mapOf<String, Any>(
                    explainResponseOpendistroPolicyIdSetting to policy.id,
                    explainResponseOpenSearchPolicyIdSetting to policy.id,
                    "index" to indexName1,
                    "index_uuid" to getUuid(indexName1),
                    "policy_id" to policy.id,
                    "enabled" to true,
                )
        val indexName2Map =
            indexName2 to
                mapOf<String, Any>(
                    explainResponseOpendistroPolicyIdSetting to policy.id,
                    explainResponseOpenSearchPolicyIdSetting to policy.id,
                    "index" to indexName2,
                    "index_uuid" to getUuid(indexName2),
                    "policy_id" to policy.id,
                    "enabled" to true,
                )
        val indexName4Map =
            indexName4 to
                mapOf<String, Any>(
                    explainResponseOpendistroPolicyIdSetting to policy.id,
                    explainResponseOpenSearchPolicyIdSetting to policy.id,
                    "index" to indexName4,
                    "index_uuid" to getUuid(indexName4),
                    "policy_id" to policy.id,
                    "enabled" to true,
                )
        val indexName5Map =
            indexName5 to
                mapOf<String, Any>(
                    explainResponseOpendistroPolicyIdSetting to policy.id,
                    explainResponseOpenSearchPolicyIdSetting to policy.id,
                    "index" to indexName5,
                    "index_uuid" to getUuid(indexName5),
                    "policy_id" to policy.id,
                    "enabled" to true,
                )
        val datastreamMap =
            ".ds-$dataStreamName-000001" to
                mapOf<String, Any>(
                    explainResponseOpendistroPolicyIdSetting to policy.id,
                    explainResponseOpenSearchPolicyIdSetting to policy.id,
                    "index" to ".ds-$dataStreamName-000001",
                    "index_uuid" to getUuid(".ds-$dataStreamName-000001"),
                    "policy_id" to policy.id,
                    "enabled" to true,
                )

        waitFor {
            val expected =
                mapOf(
                    indexName1Map,
                    indexName2Map,
                    indexName4Map,
                    indexName5Map,
                    "total_managed_indices" to 4,
                )
            // These should match all non datastream managed indices
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=$testIndexName*"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=$testIndexName-*"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=$testIndexName-search-*"))
        }

        waitFor {
            val expected =
                mapOf(
                    indexName1Map,
                    indexName2Map,
                    indexName4Map,
                    indexName5Map,
                    datastreamMap,
                    "total_managed_indices" to 5,
                )
            // These should match all managed indices including datastreams
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=*$testIndexName-*"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=*search*"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=*search-query*"))
        }

        waitFor {
            val expected =
                mapOf(
                    datastreamMap,
                    "total_managed_indices" to 1,
                )
            // These should match all datastream managed indices (and system/hidden indices)
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=.*"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=.ds-$testIndexName-*"))
        }

        waitFor {
            val expected =
                mapOf(
                    indexName4Map,
                    "total_managed_indices" to 1,
                )
            // These should match all just the single index, and validates that it does not match the 15-02-2022 index
            // i.e. if it was still matching on tokens then ["2022", "02", "15"] would match both which we don't want
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=*2022-02-15"))
            assertResponseMap(expected, getExplainMap(indexName = null, queryParams = "queryString=*2022-02-15*"))
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
                    indexName to
                        listOf(
                            explainResponseOpendistroPolicyIdSetting to policy.id::equals,
                            explainResponseOpenSearchPolicyIdSetting to policy.id::equals,
                            ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                            ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                            ManagedIndexMetaData.POLICY_ID to managedIndexConfig.policyID::equals,
                            ManagedIndexMetaData.POLICY_SEQ_NO to policy.seqNo.toInt()::equals,
                            ManagedIndexMetaData.POLICY_PRIMARY_TERM to policy.primaryTerm.toInt()::equals,
                            ManagedIndexMetaData.INDEX_CREATION_DATE to

                                fun(indexCreationDate: Any?): Boolean = (indexCreationDate as Long) > 1L,
                            StateMetaData.STATE to

                                fun(stateMetaDataMap: Any?): Boolean =
                                    assertStateEquals(
                                        StateMetaData(policy.defaultState, Instant.now().toEpochMilli()),
                                        stateMetaDataMap,
                                    ),
                            PolicyRetryInfoMetaData.RETRY_INFO to

                                fun(retryInfoMetaDataMap: Any?): Boolean =
                                    assertRetryInfoEquals(PolicyRetryInfoMetaData(false, 0), retryInfoMetaDataMap),
                            ManagedIndexMetaData.INFO to

                                fun(info: Any?): Boolean = expectedInfoString == info.toString(),
                            ManagedIndexMetaData.ENABLED to true::equals,
                        ),
                ),
                getExplainMap(indexName),
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
            "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/$indexName", emptyMap(), changePolicy.toHttpEntity(),
        )
        val deletePolicyResponse =
            client().makeRequest(
                RestRequest.Method.DELETE.toString(),
                "${IndexManagementPlugin.LEGACY_POLICY_BASE_URI}/${changePolicy.policyID}",
            )
        assertEquals("Unexpected RestStatus", RestStatus.OK, deletePolicyResponse.restStatus())

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val expectedInfoString = mapOf("message" to "Fail to load policy: ${changePolicy.policyID}").toString()

            val explainMap = getExplainMap(indexName)
            assertPredicatesOnMetaData(
                listOf(
                    indexName to
                        listOf(
                            explainResponseOpendistroPolicyIdSetting to policy.id::equals,
                            explainResponseOpenSearchPolicyIdSetting to policy.id::equals,
                            ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                            ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                            ManagedIndexMetaData.POLICY_ID to newPolicy.id::equals,
                            PolicyRetryInfoMetaData.RETRY_INFO to

                                fun(retryInfoMetaDataMap: Any?): Boolean =
                                    assertRetryInfoEquals(PolicyRetryInfoMetaData(true, 0), retryInfoMetaDataMap),
                            ManagedIndexMetaData.INFO to

                                fun(info: Any?): Boolean = expectedInfoString == info.toString(),
                            ManagedIndexMetaData.INDEX_CREATION_DATE to

                                fun(indexCreationDate: Any?): Boolean = (indexCreationDate as Long) > 1L,
                            ManagedIndexMetaData.ENABLED to true::equals,
                        ),
                ),
                explainMap,
            )
        }
    }

    fun `test show_applied_policy query parameter`() {
        val indexName = "${testIndexName}_show_applied_policy"
        val policy = createRandomPolicy()
        createIndex(indexName, policy.id)

        val expectedPolicy = policy.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE_AND_USER).toMap()
        val expected =
            mapOf(
                indexName to
                    mapOf<String, Any>(
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

    fun `test explain filter`() {
        val indexName1 = "${testIndexName}_filter1"
        val indexName2 = "${testIndexName}_filter2"

        val stateWithReadOnlyAction = randomState(actions = listOf(ReadOnlyAction(index = 0)))
        val policy1 = createPolicy(randomPolicy(states = listOf(stateWithReadOnlyAction)))

        val stateWithDeleteAction = randomState(actions = listOf(DeleteAction(index = 0)))
        val updatedStateWithReadOnlyAction =
            stateWithReadOnlyAction.copy(
                actions = listOf(stateWithReadOnlyAction.actions.first(), OpenAction(index = 1)),
                transitions = listOf(Transition(stateWithDeleteAction.name, null)),
            )
        val policy2 = createPolicy(randomPolicy(states = listOf(stateWithDeleteAction, updatedStateWithReadOnlyAction)))

        createIndex(indexName1, policy1.id)
        createIndex(indexName2, policy2.id)

        val managedIndexConfig1 = getExistingManagedIndexConfig(indexName1)
        val managedIndexConfig2 = getExistingManagedIndexConfig(indexName2)

        // init policy on job
        updateManagedIndexConfigStartTime(managedIndexConfig1)

        // verify we have policy
        waitFor { assertEquals(policy1.id, getExplainManagedIndexMetaData(indexName1).policyID) }

        // do the same for index2
        updateManagedIndexConfigStartTime(managedIndexConfig2)
        waitFor { assertEquals(policy2.id, getExplainManagedIndexMetaData(indexName2).policyID) }

        // speed up to execute set read only step
        updateManagedIndexConfigStartTime(managedIndexConfig1)

        val index1Predicates =
            indexName1 to
                listOf(
                    explainResponseOpendistroPolicyIdSetting to policy1.id::equals,
                    explainResponseOpenSearchPolicyIdSetting to policy1.id::equals,
                    ManagedIndexMetaData.INDEX to managedIndexConfig1.index::equals,
                    ManagedIndexMetaData.INDEX_UUID to managedIndexConfig1.indexUuid::equals,
                    ManagedIndexMetaData.POLICY_ID to managedIndexConfig1.policyID::equals,
                    ManagedIndexMetaData.POLICY_SEQ_NO to policy1.seqNo.toInt()::equals,
                    ManagedIndexMetaData.POLICY_PRIMARY_TERM to policy1.primaryTerm.toInt()::equals,
                    ManagedIndexMetaData.INDEX_CREATION_DATE to

                        fun(indexCreationDate: Any?): Boolean = (indexCreationDate as Long) > 1L,
                    StateMetaData.STATE to

                        fun(stateMetaDataMap: Any?): Boolean =
                            assertStateEquals(
                                StateMetaData(policy1.defaultState, Instant.now().toEpochMilli()),
                                stateMetaDataMap,
                            ),
                    ActionMetaData.ACTION to

                        fun(actionMetaDataMap: Any?): Boolean =
                            assertActionEquals(
                                ActionMetaData(
                                    name = "read_only", startTime = Instant.now().toEpochMilli(), failed = false,
                                    index = 0, consumedRetries = 0, lastRetryTime = null, actionProperties = null,
                                ),
                                actionMetaDataMap,
                            ),
                    PolicyRetryInfoMetaData.RETRY_INFO to

                        fun(retryInfoMetaDataMap: Any?): Boolean =
                            assertRetryInfoEquals(PolicyRetryInfoMetaData(false, 0), retryInfoMetaDataMap),
                    ManagedIndexMetaData.ENABLED to true::equals,
                )

        val index2Predicates =
            indexName2 to
                listOf(
                    explainResponseOpendistroPolicyIdSetting to policy2.id::equals,
                    explainResponseOpenSearchPolicyIdSetting to policy2.id::equals,
                    ManagedIndexMetaData.INDEX to managedIndexConfig2.index::equals,
                    ManagedIndexMetaData.INDEX_UUID to managedIndexConfig2.indexUuid::equals,
                    ManagedIndexMetaData.POLICY_ID to managedIndexConfig2.policyID::equals,
                    ManagedIndexMetaData.POLICY_SEQ_NO to policy2.seqNo.toInt()::equals,
                    ManagedIndexMetaData.POLICY_PRIMARY_TERM to policy2.primaryTerm.toInt()::equals,
                    ManagedIndexMetaData.INDEX_CREATION_DATE to

                        fun(indexCreationDate: Any?): Boolean = (indexCreationDate as Long) > 1L,
                    StateMetaData.STATE to

                        fun(stateMetaDataMap: Any?): Boolean =
                            assertStateEquals(
                                StateMetaData(policy2.defaultState, Instant.now().toEpochMilli()),
                                stateMetaDataMap,
                            ),
                    ActionMetaData.ACTION to

                        fun(actionMetaDataMap: Any?): Boolean =
                            assertActionEquals(
                                ActionMetaData(
                                    name = "delete", startTime = Instant.now().toEpochMilli(), failed = false,
                                    index = 0, consumedRetries = 0, lastRetryTime = null, actionProperties = null,
                                ),
                                actionMetaDataMap,
                            ),
                    PolicyRetryInfoMetaData.RETRY_INFO to

                        fun(retryInfoMetaDataMap: Any?): Boolean =
                            assertRetryInfoEquals(PolicyRetryInfoMetaData(false, 0), retryInfoMetaDataMap),
                    ManagedIndexMetaData.ENABLED to true::equals,
                )

        // check metadata for result from filtering on the first policy and its state
        waitFor {
            val filterPolicy =
                ExplainFilter(
                    policyID = policy1.id,
                    state = policy1.states[0].name,
                    failed = false,
                )

            val resp =
                client().makeRequest(
                    RestRequest.Method.POST.toString(),
                    RestExplainAction.EXPLAIN_BASE_URI, emptyMap(), filterPolicy.toHttpEntity(),
                )

            assertEquals("Unexpected RestStatus", RestStatus.OK, resp.restStatus())

            assertPredicatesOnMetaData(
                listOf(index1Predicates),
                resp.asMap(), false,
            )
        }

        // check metadata on filtering for the delete action

        // speed up to execute set read only step
        updateManagedIndexConfigStartTime(managedIndexConfig1)
        updateManagedIndexConfigStartTime(managedIndexConfig2)

        waitFor(timeout = Instant.ofEpochSecond(60)) {
            val filterPolicy =
                ExplainFilter(
                    actionType = "delete",
                    failed = false,
                )

            val resp =
                client().makeRequest(
                    RestRequest.Method.POST.toString(),
                    RestExplainAction.EXPLAIN_BASE_URI, emptyMap(), filterPolicy.toHttpEntity(),
                )

            assertEquals("Unexpected RestStatus", RestStatus.OK, resp.restStatus())

            assertPredicatesOnMetaData(
                listOf(
                    index2Predicates,
                ),
                resp.asMap(), false,
            )
        }

        removePolicyFromIndex(indexName1)
        removePolicyFromIndex(indexName2)

        // wait for job to finish
        Thread.sleep(1000)
    }

    fun `test explain filter failed index`() {
        val indexName1 = "${testIndexName}_failed"
        val indexName2 = "${testIndexName}_success"

        // for failed index
        val config = AllocationAction(require = mapOf("..//" to "value"), exclude = emptyMap(), include = emptyMap(), index = 0)
        config.configRetry = ActionRetry(0)
        val states =
            listOf(
                randomState().copy(
                    transitions = listOf(),
                    actions = listOf(config),
                ),
            )
        val invalidPolicy =
            randomPolicy().copy(
                states = states,
                defaultState = states[0].name,
            )

        // for successful index
        val stateWithReadOnlyAction = randomState(actions = listOf(ReadOnlyAction(index = 0)))
        val validPolicy = createPolicy(randomPolicy(states = listOf(stateWithReadOnlyAction)))

        createPolicy(invalidPolicy, invalidPolicy.id)
        createIndex(indexName1, invalidPolicy.id)
        createIndex(indexName2, validPolicy.id)

        val managedIndexConfig1 = getExistingManagedIndexConfig(indexName1)
        val managedIndexConfig2 = getExistingManagedIndexConfig(indexName2)

        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig1)
        waitFor { assertEquals(invalidPolicy.id, getExplainManagedIndexMetaData(indexName1).policyID) }

        updateManagedIndexConfigStartTime(managedIndexConfig2)
        waitFor { assertEquals(validPolicy.id, getExplainManagedIndexMetaData(indexName2).policyID) }

        // Change the start time so that we attempt allocation that is intended to fail
        updateManagedIndexConfigStartTime(managedIndexConfig1)
        waitFor {
            val explainFilter =
                ExplainFilter(
                    failed = true,
                )

            val resp =
                client().makeRequest(
                    RestRequest.Method.POST.toString(),
                    RestExplainAction.EXPLAIN_BASE_URI, emptyMap(), explainFilter.toHttpEntity(),
                )

            assertEquals("Unexpected RestStatus", RestStatus.OK, resp.restStatus())

            assertPredicatesOnMetaData(
                listOf(
                    indexName1 to
                        listOf(
                            explainResponseOpendistroPolicyIdSetting to invalidPolicy.id::equals,
                            explainResponseOpenSearchPolicyIdSetting to invalidPolicy.id::equals,
                            ManagedIndexMetaData.INDEX to managedIndexConfig1.index::equals,
                            ManagedIndexMetaData.INDEX_UUID to managedIndexConfig1.indexUuid::equals,
                            ManagedIndexMetaData.POLICY_ID to managedIndexConfig1.policyID::equals,
                            ManagedIndexMetaData.INDEX_CREATION_DATE to

                                fun(indexCreationDate: Any?): Boolean = (indexCreationDate as Long) > 1L,
                            StepMetaData.STEP to

                                fun(stepMetaDataMap: Any?): Boolean =
                                    assertStepEquals(
                                        StepMetaData("attempt_allocation", Instant.now().toEpochMilli(), Step.StepStatus.FAILED),
                                        stepMetaDataMap,
                                    ),
                            ManagedIndexMetaData.ENABLED to true::equals,
                        ),
                ),
                resp.asMap(), false,
            )
        }

        updateManagedIndexConfigStartTime(managedIndexConfig2)
        waitFor {
            val explainFilter =
                ExplainFilter(
                    failed = false,
                )

            val resp =
                client().makeRequest(
                    RestRequest.Method.POST.toString(),
                    RestExplainAction.EXPLAIN_BASE_URI, emptyMap(), explainFilter.toHttpEntity(),
                )

            assertEquals("Unexpected RestStatus", RestStatus.OK, resp.restStatus())

            assertPredicatesOnMetaData(
                listOf(
                    indexName2 to
                        listOf(
                            explainResponseOpendistroPolicyIdSetting to validPolicy.id::equals,
                            explainResponseOpenSearchPolicyIdSetting to validPolicy.id::equals,
                            ManagedIndexMetaData.INDEX to managedIndexConfig2.index::equals,
                            ManagedIndexMetaData.INDEX_UUID to managedIndexConfig2.indexUuid::equals,
                            ManagedIndexMetaData.POLICY_ID to managedIndexConfig2.policyID::equals,
                            ManagedIndexMetaData.INDEX_CREATION_DATE to

                                fun(indexCreationDate: Any?): Boolean = (indexCreationDate as Long) > 1L,
                            StepMetaData.STEP to

                                fun(stepMetaDataMap: Any?): Boolean =
                                    assertStepEquals(
                                        StepMetaData("set_read_only", Instant.now().toEpochMilli(), Step.StepStatus.STARTING),
                                        stepMetaDataMap,
                                    ),
                            ManagedIndexMetaData.ENABLED to true::equals,
                        ),
                ),
                resp.asMap(), false,
            )
        }

        removePolicyFromIndex(indexName1)
        removePolicyFromIndex(indexName2)

        // wait for job to finish
        Thread.sleep(1000)
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
