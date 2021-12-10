/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.NotificationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.notification.AttemptNotificationStep
import org.opensearch.script.ScriptService

class NotificationAction(
    clusterService: ClusterService,
    scriptService: ScriptService,
    client: Client,
    settings: Settings,
    managedIndexMetaData: ManagedIndexMetaData,
    config: NotificationActionConfig
) : Action(ActionConfig.ActionType.NOTIFICATION, config, managedIndexMetaData) {

    private val attemptNotificationStep = AttemptNotificationStep(clusterService, scriptService, client, settings, config, managedIndexMetaData)
    private val steps = listOf(attemptNotificationStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step = attemptNotificationStep
}
