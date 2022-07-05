/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.unit.ByteSizeValue
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

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test doc count condition`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val secondStateName = "second"
        val states = listOf(
            State("first", listOf(), listOf(Transition(secondStateName, Conditions(docCount = 5L)))),
            State(secondStateName, listOf(), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
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
        val states = listOf(
            State("first", listOf(), listOf(Transition(secondStateName, Conditions(rolloverAge = TimeValue.timeValueSeconds(30))))),
            State(secondStateName, listOf(), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
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
        val states = listOf(
            State("first", listOf(), listOf(Transition(secondStateName, Conditions(indexAge = TimeValue.timeValueDays(2), docCount = 3, size = 5 as? ByteSizeValue, cron = null, rolloverAge = TimeValue.timeValueMillis(1))))),
            State(secondStateName, listOf(), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
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

        waitFor {
            // Should have evaluated to true
            assertEquals(AttemptTransitionStep.getSuccessMessage(indexName, secondStateName), getExplainManagedIndexMetaData(indexName).info?.get("message"))
            // check all current state conditions in infomap align with index
            val infoMap = getExplainManagedIndexMetaData(indexName).info as Map<String, Any?>
            // indexAge
            val expectedCreationDate = (cat("indices/$indexName?format=json&h=creation.date.string") as List<Map<String, Any>>)[0]["creation.date.string"]
            val indexAgeMap = infoMap?.get("min_index_age") as Map<String, Any?>
            assertEquals("incorrect index age: ${indexAgeMap?.get("creationDate")}", expectedCreationDate, indexAgeMap?.get("creationDate"))
            // docCount
            assertEquals("incorrect number of docs", 3, infoMap?.get("docCount"))
            // size
            assertEquals("incorrect index size", infoMap?.get("size") as ByteSizeValue, 5 as? ByteSizeValue)
            // cron
            assertEquals("Cron field is wrong", null, infoMap?.get("cron"))
            // rolloverAge
            assertEquals("Rollover age is wrong", TimeValue.timeValueMillis(1), infoMap?.get("rolloverAge") as? TimeValue)
        }
    }
}
