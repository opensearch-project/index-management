/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ValidateDeleteIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test delete index is write index`() {
        enableValidationService()
        val index1 = "index-1"
        val alias1 = "x"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = DeleteAction(0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "DeleteAction", actions = listOf(actionConfig), transitions = listOf()))

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
        // waitFor { assertIndexExists(index1) }

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val data = getExplainValidationResult(index1)
            assertEquals(
                "Index delete action validation status is RE_VALIDATING.",
                Validate.ValidationStatus.RE_VALIDATING,
                data?.validationStatus
            )
        }
        waitFor {
            val data = getExplainValidationResult(index1)
            assertEquals(
                "Index delete action validation message is index is write index.",
                ValidateDelete.getFailedIsWriteIndexMessage(index1),
                data?.validationMessage
            )
        }
    }
}
