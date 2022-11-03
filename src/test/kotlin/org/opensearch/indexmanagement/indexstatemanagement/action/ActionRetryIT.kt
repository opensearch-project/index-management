/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.util.Locale

class ActionRetryIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    /**
     * We are forcing RollOver to fail in this Integ test.
     */
    fun `test failed action`() {
        disableValidationService()
        val testPolicy = """
        {"policy":{"description":"Default policy","default_state":"Ingest","states":[
        {"name":"Ingest","actions":[{"retry":{"count":2,"backoff":"constant","delay":"1s"},"rollover":{"min_doc_count":100}}],"transitions":[{"state_name":"Search"}]},
        {"name":"Search","actions":[],"transitions":[{"state_name":"Delete","conditions":{"min_index_age":"30d"}}]},
        {"name":"Delete","actions":[{"delete":{}}],"transitions":[]}]}}
        """.trimIndent()

        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        createPolicyJson(testPolicy, policyID)
        val expectedInfoString = mapOf("message" to AttemptRolloverStep.getFailedNoValidAliasMessage(indexName)).toString()

        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        // First execution. We need to initialize the policy.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Second execution is to fail the step once.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val managedIndexMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                ActionMetaData(
                    "rollover", managedIndexMetaData.actionMetaData?.startTime, 0, false, 1,
                    managedIndexMetaData.actionMetaData?.lastRetryTime, null
                ),
                managedIndexMetaData.actionMetaData
            )

            assertEquals(expectedInfoString, managedIndexMetaData.info.toString())
        }

        // Third execution is to fail the step second time.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val managedIndexMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                ActionMetaData(
                    "rollover", managedIndexMetaData.actionMetaData?.startTime, 0, false, 2,
                    managedIndexMetaData.actionMetaData?.lastRetryTime, null
                ),
                managedIndexMetaData.actionMetaData
            )

            assertEquals(expectedInfoString, managedIndexMetaData.info.toString())
        }

        // Fourth execution is to fail the step third time and finally fail the action.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val managedIndexMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                ActionMetaData(
                    "rollover", managedIndexMetaData.actionMetaData?.startTime, 0, true, 2,
                    managedIndexMetaData.actionMetaData?.lastRetryTime, null
                ),
                managedIndexMetaData.actionMetaData
            )

            assertEquals(expectedInfoString, managedIndexMetaData.info.toString())
        }
    }

    fun `test exponential backoff`() {
        disableValidationService()
        val testPolicy = """
        {"policy":{"description":"Default policy","default_state":"Ingest","states":[
        {"name":"Ingest","actions":[{"retry":{"count":2,"backoff":"exponential","delay":"1m"},"rollover":{"min_doc_count":100}}],"transitions":[{"state_name":"Search"}]},
        {"name":"Search","actions":[],"transitions":[{"state_name":"Delete","conditions":{"min_index_age":"30d"}}]},
        {"name":"Delete","actions":[{"delete":{}}],"transitions":[]}]}}
        """.trimIndent()

        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"

        val policyResponse = createPolicyJson(testPolicy, policyID)
        val policyResponseMap = policyResponse.asMap()
        val policySeq = policyResponseMap["_seq_no"] as Int
        val policyPrimaryTerm = policyResponseMap["_primary_term"] as Int

        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        // First execution. We need to initialize the policy.

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        // Second execution is to fail the step once.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(1, getExplainManagedIndexMetaData(indexName).actionMetaData?.consumedRetries) }

        // Third execution should not run job since we have the retry backoff.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        Thread.sleep(5000) // currently there is nothing to compare when backing off so we have to sleep
        // Fourth execution should not run job since we have the retry backoff.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // even if we ran couple times we should have backed off and only retried once.
        waitFor {
            val expectedInfoString = mapOf("message" to AttemptRolloverStep.getFailedNoValidAliasMessage(indexName)).toString()
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        explainResponseOpendistroPolicyIdSetting to policyID::equals,
                        explainResponseOpenSearchPolicyIdSetting to policyID::equals,
                        ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                        ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                        ManagedIndexMetaData.POLICY_ID to managedIndexConfig.policyID::equals,
                        ManagedIndexMetaData.POLICY_SEQ_NO to policySeq::equals,
                        ManagedIndexMetaData.POLICY_PRIMARY_TERM to policyPrimaryTerm::equals,
                        ManagedIndexMetaData.ROLLED_OVER to false::equals,
                        ManagedIndexMetaData.INDEX_CREATION_DATE to fun(indexCreationDate: Any?): Boolean = (indexCreationDate as Long) > 1L,
                        StateMetaData.STATE to fun(stateMetaDataMap: Any?): Boolean =
                            assertStateEquals(StateMetaData("Ingest", Instant.now().toEpochMilli()), stateMetaDataMap),
                        ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean =
                            assertActionEquals(
                                ActionMetaData("rollover", Instant.now().toEpochMilli(), 0, false, 1, null, null),
                                actionMetaDataMap
                            ),
                        StepMetaData.STEP to fun(stepMetaDataMap: Any?): Boolean =
                            assertStepEquals(
                                StepMetaData("attempt_rollover", Instant.now().toEpochMilli(), Step.StepStatus.FAILED),
                                stepMetaDataMap
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
}
