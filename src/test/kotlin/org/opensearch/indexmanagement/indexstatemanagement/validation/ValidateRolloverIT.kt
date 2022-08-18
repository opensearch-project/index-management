/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestRetryFailedManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ValidateRolloverIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    // status: PASSED
    fun `test skip rollover`() {
        enableValidationService()
        val index1 = "index-1"
        val alias1 = "x"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        createIndex(index1, policyID)
        changeAlias(index1, alias1, "add", true)
        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)

        val managedIndexConfig = getExistingManagedIndexConfig(index1)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }

        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_SKIP.key, "true")

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$index1"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val data = getExplainValidationResult(index1)
            assertEquals(
                "Index rollover validation status is pass.",
                Validate.ValidationStatus.PASSED, data?.validationStatus
            )
            assertEquals(
                "Index rollover validation message is skipped rollover",
                ValidateRollover.getSkipRolloverMessage(index1), data?.validationMessage
            )
        }
    }

    // status: PASSED
    fun `test rollover has already been rolled over`() {
        enableValidationService()
        val aliasName = "${testIndexName}_alias"
        val indexNameBase = "${testIndexName}_index"
        val index1 = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = RolloverAction(null, null, null, null, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        // create index defaults
        createIndex(index1, policyID, aliasName)

        val managedIndexConfig = getExistingManagedIndexConfig(index1)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }

        // Rollover the alias manually before ISM tries to roll it over
        rolloverIndex(aliasName)

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val data = getExplainValidationResult(index1)
            assertEquals(
                "Index rollover validation status is PASSED.",
                Validate.ValidationStatus.PASSED, data?.validationStatus
            )
            assertEquals(
                "Index rollover validation message is already rolled over",
                ValidateRollover.getAlreadyRolledOverMessage(index1, aliasName), data?.validationMessage
            )
        }
        assertTrue("New rollover index does not exist.", indexExists("$indexNameBase-000002"))
    }

    // status: RE_VALIDATING
    fun `test rollover does not have rollover alias index setting`() {
        enableValidationService()
        val index1 = "index-1"
        val index2 = "index-2"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        createIndex(index1, policyID)
        createIndex(index2, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(index1)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val data = getExplainValidationResult(index1)
            assertEquals(
                "Index rollover validation status is RE_VALIDATING",
                Validate.ValidationStatus.RE_VALIDATING, data?.validationStatus
            )
            assertEquals(
                "Index rollover validation message is no alias index setting",
                ValidateRollover.getFailedNoValidAliasMessage(index1), data?.validationMessage
            )
        }
    }

    // status: RE_VALIDATING
    fun `test rollover not write index`() {
        enableValidationService()
        val index1 = "index-1"
        val index2 = "index-2"
        val alias1 = "x"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        createIndex(index1, policyID)
        changeAlias(index1, alias1, "add")
        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)
        createIndex(index2, policyID)
        changeAlias(index2, alias1, "add", true)
        updateIndexSetting(index2, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)

        val managedIndexConfig = getExistingManagedIndexConfig(index1)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val data = getExplainValidationResult(index1)
            assertEquals(
                "Index rollover validation status is RE_VALIDATING.",
                Validate.ValidationStatus.RE_VALIDATING, data?.validationStatus
            )
            assertEquals(
                "Index rollover validation message is not write index",
                ValidateRollover.getFailedWriteIndexMessage(index1), data?.validationMessage
            )
        }
    }
}
