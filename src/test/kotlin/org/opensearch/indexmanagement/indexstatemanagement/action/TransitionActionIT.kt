/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Conditions
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.transition.AttemptTransitionStep
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class TransitionActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test doc count condition`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val secondStateName = "second"
        val states =
            listOf(
                State("first", listOf(), listOf(Transition(secondStateName, Conditions(docCount = 5L)))),
                State(secondStateName, listOf(), listOf()),
            )

        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Initializing the policy/metadata
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Evaluating transition conditions for first time
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Should not have evaluated to true
        waitFor { assertEquals(AttemptTransitionStep.getEvaluatingMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // Add 6 documents (>5)
        insertSampleData(indexName, 6)

        // Evaluating transition conditions for second time
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Should have evaluated to true
        waitFor { assertEquals(AttemptTransitionStep.getSuccessMessage(indexName, secondStateName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }
    }

    fun `test rollover age transition for index with no rollover fails`() {
        val indexName = "${testIndexName}_rollover_age_no_rollover"
        val policyID = "${testIndexName}_rollover_age_no_rollover_policy"
        val secondStateName = "second"
        val states =
            listOf(
                State("first", listOf(), listOf(Transition(secondStateName, Conditions(rolloverAge = TimeValue.timeValueSeconds(30))))),
                State(secondStateName, listOf(), listOf()),
            )

        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Initializing the policy/metadata
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Evaluating transition conditions for first time
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Should fail because it attempted to use the rollover age and the index has not been rolled over
        waitFor { assertEquals(AttemptTransitionStep.getFailedRolloverDateMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }
    }

    fun `test rollover age transition for index`() {
        val indexName = "${testIndexName}_rollover_age-01"
        val policyID = "${testIndexName}_rollover_age_policy"
        val alias = "foo-alias"
        val secondStateName = "second"
        val states =
            listOf(
                State("first", listOf(), listOf(Transition(secondStateName, Conditions(rolloverAge = TimeValue.timeValueMillis(1))))),
                State(secondStateName, listOf(), listOf()),
            )

        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, alias)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Initializing the policy/metadata
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Rollover the index
        rolloverIndex(alias)

        // Evaluating transition conditions for first time
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Should have evaluated to true
        waitFor { assertEquals(AttemptTransitionStep.getSuccessMessage(indexName, secondStateName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }
    }

    fun `test noAlias transition condition`() {
        val indexName = "${testIndexName}_no_alias"
        val policyID = "${testIndexName}_no_alias_policy"
        val secondStateName = "second"
        val states =
            listOf(
                State("first", listOf(), listOf(Transition(secondStateName, Conditions(noAlias = true)))),
                State(secondStateName, listOf(), listOf()),
            )

        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Initializing the policy/metadata
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Evaluating transition conditions for first time (should succeed, no alias)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(AttemptTransitionStep.getSuccessMessage(indexName, secondStateName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // Create a new index and add an alias, then attach the policy with noAlias=true (should NOT transition)
        val indexWithAlias = "${testIndexName}_with_alias"
        createIndex(indexWithAlias, policyID, "foo-alias")
        addPolicyToIndex(indexWithAlias, policyID)
        val managedIndexConfigWithAlias = getExistingManagedIndexConfig(indexWithAlias)
        updateManagedIndexConfigStartTime(managedIndexConfigWithAlias)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexWithAlias).policyID) }
        updateManagedIndexConfigStartTime(managedIndexConfigWithAlias)
        // Should not transition because alias exists and noAlias=true
        waitFor { assertEquals(AttemptTransitionStep.getEvaluatingMessage(indexWithAlias), getExplainManagedIndexMetaData(indexWithAlias).info?.get("message")) }

        // Now test noAlias=false: should transition if alias exists
        val indexWithAlias2 = "${indexWithAlias}_2"
        val policyIDWithNoAliasFalse = "${testIndexName}_no_alias_false_policy"
        val statesWithNoAliasFalse =
            listOf(
                State("first", listOf(), listOf(Transition(secondStateName, Conditions(noAlias = false)))),
                State(secondStateName, listOf(), listOf()),
            )
        val policyWithAlias =
            Policy(
                id = policyIDWithNoAliasFalse,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = statesWithNoAliasFalse[0].name,
                states = statesWithNoAliasFalse,
            )
        createPolicy(policyWithAlias, policyIDWithNoAliasFalse)
        waitFor { assertNotNull(getPolicy(policyIDWithNoAliasFalse)) }
        createIndex(indexWithAlias2, policyIDWithNoAliasFalse, "foo-alias-2")
        addPolicyToIndex(indexWithAlias2, policyIDWithNoAliasFalse)
        val managedIndexConfigWithAlias2 = getExistingManagedIndexConfig(indexWithAlias2)
        updateManagedIndexConfigStartTime(managedIndexConfigWithAlias2)
        waitFor { assertEquals(policyIDWithNoAliasFalse, getExplainManagedIndexMetaData(indexWithAlias2).policyID) }
        updateManagedIndexConfigStartTime(managedIndexConfigWithAlias2)
        // Should transition because alias exists and noAlias=false
        waitFor {
            assertEquals(
                AttemptTransitionStep.getSuccessMessage(indexWithAlias2, secondStateName),
                getExplainManagedIndexMetaData(
                    indexWithAlias2,
                ).info?.get("message"),
            )
        }
    }

    fun `test minStateAge transition occurs after elapsed time`() {
        val indexName = "${testIndexName}_min_state_age"
        val policyID = "${testIndexName}_min_state_age_policy"
        val secondStateName = "second"
        val states =
            listOf(
                State("first", listOf(), listOf(Transition(secondStateName, Conditions(minStateAge = TimeValue.timeValueSeconds(5))))),
                State(secondStateName, listOf(), listOf()),
            )
        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Initialising policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        // should not transition immediately
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptTransitionStep.getEvaluatingMessage(indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }
        // Wait for min_state_age to elapse
        Thread.sleep(5500)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        // Should transition now
        waitFor {
            assertEquals(
                AttemptTransitionStep.getSuccessMessage(indexName, secondStateName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }
    }
}
