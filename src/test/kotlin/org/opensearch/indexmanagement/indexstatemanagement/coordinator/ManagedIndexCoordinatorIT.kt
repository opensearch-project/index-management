/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.coordinator

import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ForceMergeAction
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge.WaitForForceMergeStep
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import org.opensearch.indexmanagement.indexstatemanagement.util.TOTAL_MANAGED_INDICES
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertFailsWith

class ManagedIndexCoordinatorIT : IndexStateManagementRestTestCase() {

    fun `test creating index with valid policy_id`() {
        val policy = createRandomPolicy()
        val (index, policyID) = createIndex(policyID = policy.id)
        waitFor {
            val managedIndexConfig = getManagedIndexConfig(index)
            assertNotNull("Did not create ManagedIndexConfig", managedIndexConfig)
            assertNotNull("Invalid policy_id used", policyID)
            assertEquals("Has incorrect policy_id", policyID, managedIndexConfig!!.policyID)
            assertEquals("Has incorrect index", index, managedIndexConfig.index)
            assertEquals("Has incorrect name", index, managedIndexConfig.name)
        }
    }

    fun `test first time add policy to index will fail without an existing policy`() {
        assertFailsWith(Exception::class, "add policy is expected to fail when called with non existent policy") {
            createIndex()
        }
    }

    fun `test deleting index will remove managed-index`() {
        val policy = createRandomPolicy()
        val (index) = createIndex(policyID = policy.id)
        waitFor {
            val afterCreateConfig = getManagedIndexConfig(index)
            assertNotNull("Did not create ManagedIndexConfig", afterCreateConfig)
            deleteIndex(index)
        }

        waitFor {
            val afterDeleteConfig = getManagedIndexConfig(index)
            assertNull("Did not delete ManagedIndexConfig", afterDeleteConfig)
        }
    }

    fun `test managed index metadata is cleaned up after removing policy`() {
        disableValidationService()
        val policy = createRandomPolicy()
        val (index) = createIndex(policyID = policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(index)

        // Speed up execution to initialize policy on job
        // Loading policy will fail but ManagedIndexMetaData will be updated
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Verify ManagedIndexMetaData contains information
        waitFor {
            assertPredicatesOnMetaData(
                listOf(index to listOf(ManagedIndexMetaData.POLICY_ID to policy.id::equals)),
                getExplainMap(index),
                false
            )
        }

        // Remove policy_id from index
        removePolicyFromIndex(index)

        // Verify ManagedIndexMetaData has been cleared
        // Only ManagedIndexSettings.POLICY_ID set to null should be left in explain output
        waitFor {
            assertPredicatesOnMetaData(
                listOf(
                    index to listOf(
                        explainResponseOpendistroPolicyIdSetting to fun(policyID: Any?): Boolean =
                            policyID == null,
                        explainResponseOpenSearchPolicyIdSetting to fun(policyID: Any?): Boolean =
                            policyID == null,
                        ManagedIndexMetaData.ENABLED to fun(enabled: Any?): Boolean = enabled == null
                    )
                ),
                getExplainMap(index),
                true
            )
        }
    }

    fun `test managed-index metadata is cleaned up after index deleted`() {
        val policy = createRandomPolicy()
        val (index) = createIndex(policyID = policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(index)

        // Speed up execution to initialize policy on job
        // Loading policy will fail but ManagedIndexMetaData will be updated
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Verify ManagedIndexMetaData contains information
        waitFor {
            assertPredicatesOnMetaData(
                listOf(index to listOf(ManagedIndexMetaData.POLICY_ID to policy.id::equals)),
                getExplainMap(index),
                false
            )
        }

        deleteIndex(index)

        // Verify ManagedIndexMetadata has been cleared
        waitFor {
            val expected = mapOf(TOTAL_MANAGED_INDICES to 0)
            waitFor {
                assertEquals(expected, getExplainMap(null))
            }
        }
    }

    fun `test disabling and reenabling ism`() {
        val indexName = "test_disable_ism_index-000001"
        val policyID = "test_policy_1"

        // Create a policy with one State that performs rollover
        val rolloverActionConfig = RolloverAction(index = 0, minDocs = 5, minAge = null, minSize = null, minPrimaryShardSize = null)
        val states =
            listOf(State(name = "RolloverState", actions = listOf(rolloverActionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$policyID description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, "some_alias")

        // Add 5 documents so rollover condition will succeed
        insertSampleData(indexName, docCount = 5)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds and init policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policy.id, getExplainManagedIndexMetaData(indexName).policyID) }

        // Expect the Explain API to show an initialized ManagedIndexMetaData with the default state from the policy
        waitFor { assertEquals(policy.defaultState, getExplainManagedIndexMetaData(indexName).stateMetaData?.name) }

        // Disable Index State Management
        updateClusterSetting(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.key, "false")

        // Speed up to next execution where job should get disabled
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Confirm job was disabled
        val disabledManagedIndexConfig: ManagedIndexConfig = waitFor {
            val config = getManagedIndexConfigByDocId(managedIndexConfig.id)
            assertEquals("ManagedIndexConfig was not disabled", false, config!!.enabled)
            config
        }

        // Speed up to next execution and confirm that Explain API still shows information of policy initialization
        updateManagedIndexConfigStartTime(disabledManagedIndexConfig)

        waitFor {
            val expectedInfoString = mapOf("message" to "Successfully initialized policy: $policyID").toString()
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ManagedIndexMetaData.INDEX to indexName::equals,
                        ManagedIndexMetaData.POLICY_ID to policyID::equals,
                        ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                    )
                ),
                getExplainMap(indexName),
                false
            )
        }

        // Re-enable Index State Management
        updateClusterSetting(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.key, "true")

        // Confirm job was re-enabled
        val enabledManagedIndexConfig: ManagedIndexConfig = waitFor {
            val config = getManagedIndexConfigByDocId(disabledManagedIndexConfig.id)
            assertEquals("ManagedIndexConfig was not re-enabled", true, config!!.enabled)
            config
        }

        updateManagedIndexConfigStartTime(enabledManagedIndexConfig, retryOnConflict = 4)

        waitFor {
            assertEquals(
                AttemptRolloverStep.getSuccessMessage(indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }

    fun `test not disabling ism on unsafe step`() {
        val indexName = "test_safe_disable_ism"
        val policyID = "test_policy_1"

        // Create a policy with one State that performs force_merge and another State that deletes the index
        val forceMergeActionConfig = ForceMergeAction(index = 0, maxNumSegments = 1)
        val deleteActionConfig = DeleteAction(index = 0)
        val states = listOf(
            State(
                name = "ForceMergeState",
                actions = listOf(forceMergeActionConfig),
                transitions = listOf(Transition(stateName = "DeleteState", conditions = null))
            ),
            State(name = "DeleteState", actions = listOf(deleteActionConfig), transitions = listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$policyID description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        // Add sample data to increase segment count, passing in a delay to ensure multiple segments get created
        insertSampleData(indexName, 3, 1000)

        waitFor {
            assertTrue(
                "Segment count for [$indexName] was less than expected",
                validateSegmentCount(indexName, min = 2)
            )
        }

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Expect the Explain API to show an initialized ManagedIndexMetaData with the default state from the policy
        waitFor { assertEquals(policy.defaultState, getExplainManagedIndexMetaData(indexName).stateMetaData?.name) }

        // Second execution: Index is set to read-only for force_merge
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }

        // Third execution: Force merge operation is kicked off
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Verify maxNumSegments is set in action properties when kicking off force merge
        waitFor {
            assertEquals(
                "maxNumSegments not set in ActionProperties",
                forceMergeActionConfig.maxNumSegments,
                getExplainManagedIndexMetaData(indexName).actionMetaData?.actionProperties?.maxNumSegments
            )
        }

        // Disable Index State Management
        updateClusterSetting(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.key, "false")

        // Fourth execution: WaitForForceMergeStep is not safe to disable on, so the job should not disable yet
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Confirm we successfully executed the WaitForForceMergeStep
        waitFor {
            assertEquals(
                WaitForForceMergeStep.getSuccessMessage(indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // Confirm job was not disabled
        assertEquals("ManagedIndexConfig was disabled early", true, getExistingManagedIndexConfig(indexName).enabled)

        // Validate segments were merged
        assertTrue(
            "Segment count for [$indexName] after force merge is incorrect",
            validateSegmentCount(indexName, min = 1, max = 1)
        )

        // Fifth execution: Attempt transition, which is safe to disable on, so job should be disabled
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Explain API info should still be that of the last executed Step
        waitFor {
            assertEquals(
                WaitForForceMergeStep.getSuccessMessage(indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // Confirm job was disabled
        val disabledManagedIndexConfig: ManagedIndexConfig = waitFor {
            val config = getExistingManagedIndexConfig(indexName)
            assertEquals("ManagedIndexConfig was not disabled", false, config.enabled)
            config
        }

        // Speed up to next execution to confirm Explain API still shows information of the last executed step (WaitForForceMergeStep)
        updateManagedIndexConfigStartTime(disabledManagedIndexConfig)

        waitFor {
            val expectedInfoString = mapOf("message" to WaitForForceMergeStep.getSuccessMessage(indexName)).toString()
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ManagedIndexMetaData.INDEX to indexName::equals,
                        ManagedIndexMetaData.POLICY_ID to policyID::equals,
                        ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                    )
                ),
                getExplainMap(indexName),
                false
            )
        }
    }
}
