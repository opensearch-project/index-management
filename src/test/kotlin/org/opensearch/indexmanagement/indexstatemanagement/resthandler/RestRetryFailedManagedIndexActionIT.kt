/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction
import org.opensearch.indexmanagement.indexstatemanagement.randomForceMergeActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILED_INDICES
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILURES
import org.opensearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.util.Locale

class RestRetryFailedManagedIndexActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test missing indices`() {
        try {
            client().makeRequest(RestRequest.Method.POST.toString(), RestRetryFailedManagedIndexAction.RETRY_BASE_URI)
            fail("Expected a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "illegal_argument_exception", "reason" to "Missing indices")
                    ),
                    "type" to "illegal_argument_exception",
                    "reason" to "Missing indices"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test index list`() {
        val indexName = "${testIndexName}_movies"
        val indexName1 = "${indexName}_1"
        val indexName2 = "${indexName}_2"
        val indexName3 = "${testIndexName}_some_other_test"
        val policy = createRandomPolicy()
        createIndex(indexName, null)
        createIndex(indexName1, policy.id)
        createIndex(indexName2, null)
        createIndex(indexName3, null)

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName,$indexName1"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUuid(indexName),
                    "reason" to "This index is not being managed."
                ),
                mapOf(
                    "index_name" to indexName1,
                    "index_uuid" to getUuid(indexName1),
                    "reason" to "This index has no metadata information"
                )
            )
        )
        assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
    }

    fun `test index pattern`() {
        val indexName = "${testIndexName}_video"
        val indexName1 = "${indexName}_1"
        val indexName2 = "${indexName}_2"
        val indexName3 = "${testIndexName}_some_other_test_2"
        val policy = createRandomPolicy()
        createIndex(indexName, null)
        createIndex(indexName1, null)
        createIndex(indexName2, policy.id)
        createIndex(indexName3, null)

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName*"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUuid(indexName),
                    "reason" to "This index is not being managed."
                ),
                mapOf(
                    "index_name" to indexName1,
                    "index_uuid" to getUuid(indexName1),
                    "reason" to "This index is not being managed."
                ),
                mapOf(
                    "index_name" to indexName2,
                    "index_uuid" to getUuid(indexName2),
                    "reason" to "This index has no metadata information"
                )
            )
        )
        assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
    }

    fun `test index not being managed`() {
        val indexName = "${testIndexName}_games"
        createIndex(indexName, null)
        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUuid(indexName),
                    "reason" to "This index is not being managed."
                )
            )
        )
        assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
    }

    fun `test index has no metadata`() {
        val indexName = "${testIndexName}_players"
        val policy = createRandomPolicy()
        createIndex(indexName, policy.id)

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUuid(indexName),
                    "reason" to "This index has no metadata information"
                )
            )
        )
        assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
    }

    fun `test index not failed`() {
        val indexName = "${testIndexName}_classic"
        val policy = createRandomPolicy()
        createIndex(indexName, policyID = policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val response = client().makeRequest(
                RestRequest.Method.POST.toString(),
                "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName"
            )
            assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
            val actualMessage = response.asMap()
            val expectedErrorMessage = mapOf(
                FAILURES to true,
                UPDATED_INDICES to 0,
                FAILED_INDICES to listOf(
                    mapOf(
                        "index_name" to indexName,
                        "index_uuid" to getUuid(indexName),
                        "reason" to "This index is not in failed state."
                    )
                )
            )
            assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
        }
    }

    fun `test index failed`() {
        val indexName = "${testIndexName}_blueberry"
        val config = AllocationAction(require = mapOf("..//" to "value"), exclude = emptyMap(), include = emptyMap(), index = 0)
        config.configRetry = ActionRetry(0)
        val states = listOf(
            randomState().copy(
                transitions = listOf(),
                actions = listOf(config)
            )
        )
        val invalidPolicy = randomPolicy().copy(
            states = states,
            defaultState = states[0].name
        )
        createPolicy(invalidPolicy, invalidPolicy.id)
        createIndex(indexName, invalidPolicy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(invalidPolicy.id, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so we attempt allocation that is intended to fail
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(Step.StepStatus.FAILED, getExplainManagedIndexMetaData(indexName).stepMetaData?.stepStatus)
        }

        waitFor {
            val response = client().makeRequest(
                RestRequest.Method.POST.toString(),
                "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName"
            )
            assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
            val actualMessage = response.asMap()
            val expectedErrorMessage = mapOf(
                UPDATED_INDICES to 1,
                FAILURES to false,
                FAILED_INDICES to emptyList<Map<String, Any>>()
            )
            assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
        }
    }

    fun `test reset action start time`() {
        val indexName = "${testIndexName}_drewberry"
        val policyID = "${testIndexName}_policy_1"
        val action = randomForceMergeActionConfig(maxNumSegments = 1)
        action.configRetry = ActionRetry(0)
        val policy = randomPolicy(states = listOf(randomState(actions = listOf(action))))
        createPolicy(policy, policyId = policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // init policy on job
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // verify we have policy
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // speed up to execute set read only force merge step
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean =
                            assertActionEquals(
                                ActionMetaData(
                                    name = "force_merge", startTime = Instant.now().toEpochMilli(), failed = false,
                                    index = 0, consumedRetries = 0, lastRetryTime = null, actionProperties = null
                                ),
                                actionMetaDataMap
                            )
                    )
                ),
                getExplainMap(indexName), false
            )
        }

        // close the index to cause next execution to fail
        closeIndex(indexName)

        // speed up to execute attempt call force merge step
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // verify failed and save the startTime
        var firstStartTime: Long = Long.MAX_VALUE
        waitFor {
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean {
                            @Suppress("UNCHECKED_CAST")
                            actionMetaDataMap as Map<String, Any>
                            firstStartTime = actionMetaDataMap[ManagedIndexMetaData.START_TIME] as Long
                            return assertActionEquals(
                                ActionMetaData(
                                    name = "force_merge", startTime = Instant.now().toEpochMilli(), failed = true,
                                    index = 0, consumedRetries = 0, lastRetryTime = null, actionProperties = null
                                ),
                                actionMetaDataMap
                            )
                        }
                    )
                ),
                getExplainMap(indexName), false
            )
        }

        // retry
        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val expectedErrorMessage = mapOf(
            UPDATED_INDICES to 1,
            FAILURES to false,
            FAILED_INDICES to emptyList<Map<String, Any>>()
        )
        assertAffectedIndicesResponseIsEqual(expectedErrorMessage, response.asMap())

        // verify actionStartTime was reset to null
        assertPredicatesOnMetaData(
            listOf(
                indexName to listOf(
                    ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean {
                        @Suppress("UNCHECKED_CAST")
                        actionMetaDataMap as Map<String, Any>
                        return actionMetaDataMap[ManagedIndexMetaData.START_TIME] as Long? == null
                    }
                )
            ),
            getExplainMap(indexName), false
        )

        // should execute and set the startTime again
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // the new startTime should be greater than the first start time
        waitFor {
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean {
                            @Suppress("UNCHECKED_CAST")
                            actionMetaDataMap as Map<String, Any>
                            return actionMetaDataMap[ManagedIndexMetaData.START_TIME] as Long > firstStartTime
                        }
                    )
                ),
                getExplainMap(indexName), false
            )
        }
    }
}
