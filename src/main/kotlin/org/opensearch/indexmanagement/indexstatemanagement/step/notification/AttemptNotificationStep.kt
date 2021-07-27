/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.notification

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.SendNotificationResponse
import org.opensearch.commons.notifications.model.ChannelMessage
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.Feature
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.NotificationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript

class AttemptNotificationStep(
    val clusterService: ClusterService,
    val scriptService: ScriptService,
    val client: Client,
    val settings: Settings,
    val config: NotificationActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_notification", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private val hostDenyList = settings.getAsList(ManagedIndexSettings.HOST_DENY_LIST)

    override fun isIdempotent() = false

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute(): AttemptNotificationStep {
        try {
            withContext(Dispatchers.IO) {
                config.destination?.publish(null, compileTemplate(config.messageTemplate, managedIndexMetaData), hostDenyList)
                config.channel?.let {
                    NotificationsPluginInterface.sendNotification(
                        (client as NodeClient),
                        EventSource("Index Management-ISM-Notification Action", managedIndexMetaData.indexUuid, Feature.INDEX_MANAGEMENT, SeverityType.INFO),
                        ChannelMessage(compileTemplate(config.messageTemplate, managedIndexMetaData), null, null),
                        listOf(it.id),
                        object : ActionListener<SendNotificationResponse> {
                            override fun onResponse(response: SendNotificationResponse) {
                                logger.info("Notification successfully published to channel ${it.id}, notificationId: ${response.notificationId}")
                            }

                            override fun onFailure(e: Exception) {
                                logger.error("Notification failed to publish to channel ${it.id}", e)
                                throw e
                            }
                        }
                    )
                }
            }

            // publish internally throws an error for any invalid responses so its safe to assume if we reach this point it was successful
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(indexName))
        } catch (e: Exception) {
            handleException(e)
        }

        return this
    }

    private fun handleException(e: Exception) {
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    private fun compileTemplate(template: Script, managedIndexMetaData: ManagedIndexMetaData): String {
        return scriptService.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to managedIndexMetaData.convertToMap()))
            .execute()
    }

    companion object {
        fun getFailedMessage(index: String) = "Failed to send notification [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully sent notification [index=$index]"
    }
}
