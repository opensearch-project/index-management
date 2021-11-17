/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.model.action.SnapshotActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.snapshot.AttemptSnapshotStep
import org.opensearch.indexmanagement.indexstatemanagement.step.snapshot.WaitForSnapshotStep
import org.opensearch.script.ScriptService

class SnapshotAction(
    clusterService: ClusterService,
    scriptService: ScriptService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: SnapshotActionConfig
) : Action(ActionType.SNAPSHOT, config, managedIndexMetaData) {
    private val attemptSnapshotStep = AttemptSnapshotStep(clusterService, scriptService, client, config, managedIndexMetaData)
    private val waitForSnapshotStep = WaitForSnapshotStep(clusterService, client, config, managedIndexMetaData)

    override fun getSteps(): List<Step> = listOf(attemptSnapshotStep, waitForSnapshotStep)

    @Suppress("ReturnCount")
    override fun getStepToExecute(): Step {
        // If stepMetaData is null, return the first step
        val stepMetaData = managedIndexMetaData.stepMetaData ?: return attemptSnapshotStep

        // If the current step has completed, return the next step
        if (stepMetaData.stepStatus == Step.StepStatus.COMPLETED) {
            return when (stepMetaData.name) {
                AttemptSnapshotStep.name -> waitForSnapshotStep
                else -> attemptSnapshotStep
            }
        }

        return when (stepMetaData.name) {
            AttemptSnapshotStep.name -> attemptSnapshotStep
            else -> waitForSnapshotStep
        }
    }
}
