/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.CloseAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ValidateCloseIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test close index is not valid`() {
        enableValidationService()
        val index1 = "index-1"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = CloseAction(0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "CloseAction", actions = listOf(actionConfig), transitions = listOf()))

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

        val managedIndexConfig = getExistingManagedIndexConfig(index1)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }
        // waitFor { assertIndexExists(index1) }

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val data = getExplainValidationResult(index1)
            assertEquals(
                "Index close action validation status is RE_VALIDATING.",
                Validate.ValidationStatus.RE_VALIDATING,
                data.validationStatus
            )
        }
        waitFor {
            val data = getExplainValidationResult(index1)
            assertEquals(
                "Index close action validation message is index is invalid.",
                ValidateClose.getNoIndexMessage(index1),
                data.validationMessage
            )
        }
    }
}
