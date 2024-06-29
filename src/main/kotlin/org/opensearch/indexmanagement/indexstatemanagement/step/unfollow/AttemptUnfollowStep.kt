/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.unfollow

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.replication.ReplicationPluginInterface
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.transport.RemoteTransportException
class AttemptUnfollowStep : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        try {
            val stopIndexReplicationRequestObj = StopIndexReplicationRequest(indexName)
            /*val response: AcknowledgedResponse =
                ReplicationPluginInterface.suspendUntil {
                    this.stopReplication(
                        context.client as NodeClient,
                        stopIndexReplicationRequestObj,
                        it,
                    )
                }*/
            val response = performStopAction(context.client as NodeClient, stopIndexReplicationRequestObj)
            if (response.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getSuccessMessage(indexName))
            } else {
                val message = getFailedMessage(indexName)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
            }
        } catch (e: RemoteTransportException) {
            val cause = ExceptionsHelper.unwrapCause(e)
            if (cause is SnapshotInProgressException) {
                handleSnapshotException(indexName, cause)
            } else {
                handleException(indexName, cause as Exception)
            }
        } catch (e: SnapshotInProgressException) {
            handleSnapshotException(indexName, e)
        } catch (e: Exception) {
            handleException(indexName, e)
        }
        return this
    }

    internal suspend fun performStopAction(client: NodeClient, request: StopIndexReplicationRequest): AcknowledgedResponse {
        val response: AcknowledgedResponse =
            ReplicationPluginInterface.suspendUntil {
                this.stopReplication(
                    client,
                    request,
                    it,
                )
            }
        return response
    }
    private fun handleSnapshotException(indexName: String, e: SnapshotInProgressException) {
        val message = getSnapshotMessage(indexName)
        logger.warn(message, e)
        stepStatus = StepStatus.CONDITION_NOT_MET
        info = mapOf("message" to message)
    }

    private fun handleException(indexName: String, e: Exception) {
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info,
        )
    }

    override fun isIdempotent() = true

    companion object {
        const val name = "attempt_unfollow"

        fun getFailedMessage(index: String) = "Failed to unfollow index [index=$index]"

        fun getSuccessMessage(index: String) = "Successfully unfollowed index [index=$index]"

        fun getSnapshotMessage(index: String) = "Index had snapshot in progress, retrying unfollowing [index=$index]"
    }
}
