/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.transition

import org.apache.logging.log4j.LogManager
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData

class AttemptTransitionStep : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stateName: String? = null
    private var stepStatus = StepStatus.STARTING
    private var policyCompleted: Boolean = false
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        // TODO: Fix this to actually evaluate the conditions
        stepStatus = StepStatus.COMPLETED
        policyCompleted = true
        return this
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            policyCompleted = policyCompleted,
            transitionTo = stateName,
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            info = info
        )
    }

    override fun isIdempotent() = true

    companion object {
        const val name = "attempt_transition_step"
        // TODO: Fix me
        fun getSuccessMessage(indexName: String, state: String) = ""
        fun getEvaluatingMessage(indexName: String) = ""
        fun getFailedRolloverDateMessage(indexName: String) = ""
    }
}
