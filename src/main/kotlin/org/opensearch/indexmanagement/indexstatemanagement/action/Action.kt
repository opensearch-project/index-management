/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.step.Step

abstract class Action(val type: ActionType, val config: ActionConfig, val managedIndexMetaData: ManagedIndexMetaData) {

    abstract fun getSteps(): List<Step>

    abstract fun getStepToExecute(): Step

    fun isLastStep(stepName: String): Boolean = getSteps().last().name == stepName

    fun isFirstStep(stepName: String?): Boolean = getSteps().first().name == stepName
}
