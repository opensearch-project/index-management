/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.model.action.IndexPriorityActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.indexpriority.AttemptSetIndexPriorityStep

class IndexPriorityAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: IndexPriorityActionConfig
) : Action(ActionType.INDEX_PRIORITY, config, managedIndexMetaData) {

    private val attemptSetIndexPriorityStep = AttemptSetIndexPriorityStep(clusterService, client, config, managedIndexMetaData)
    private val steps = listOf(attemptSetIndexPriorityStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step = attemptSetIndexPriorityStep
}
