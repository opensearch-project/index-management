/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ReadWriteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.readwrite.SetReadWriteStep

class ReadWriteAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: ReadWriteActionConfig
) : Action(ActionType.READ_WRITE, config, managedIndexMetaData) {

    private val setReadWriteStep = SetReadWriteStep(clusterService, client, config, managedIndexMetaData)
    private val steps = listOf(setReadWriteStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step {
        return setReadWriteStep
    }
}
