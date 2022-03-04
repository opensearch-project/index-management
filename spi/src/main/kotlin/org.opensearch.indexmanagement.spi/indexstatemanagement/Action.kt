/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement

import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import java.time.Instant

abstract class Action(
    val type: String,
    val actionIndex: Int
) : ToXContentObject, Writeable {

    var configTimeout: ActionTimeout? = null
    var configRetry: ActionRetry? = ActionRetry(DEFAULT_RETRIES)
    var customAction: Boolean = false

    final override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        configTimeout?.toXContent(builder, params)
        configRetry?.toXContent(builder, params)
        // Include a "custom" object wrapper for custom actions to allow extensions to put arbitrary action configs in the config
        // index. The EXCLUDE_CUSTOM_FIELD_PARAM is used to not include this wrapper in api responses
        if (customAction && !params.paramAsBoolean(EXCLUDE_CUSTOM_FIELD_PARAM, false)) builder.startObject(CUSTOM_ACTION_FIELD)
        populateAction(builder, params)
        if (customAction && !params.paramAsBoolean(EXCLUDE_CUSTOM_FIELD_PARAM, false)) builder.endObject()
        return builder.endObject()
    }

    /**
     * The implementer of Action can change this method to correctly serialize the internals of the action
     * when stored internally or returned as response
     */
    open fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type).endObject()
    }

    final override fun writeTo(out: StreamOutput) {
        out.writeString(type)
        out.writeOptionalWriteable(configTimeout)
        out.writeOptionalWriteable(configRetry)
        populateAction(out)
    }

    fun getUpdatedActionMetadata(managedIndexMetaData: ManagedIndexMetaData, stateName: String): ActionMetaData {
        val stateMetaData = managedIndexMetaData.stateMetaData
        val actionMetaData = managedIndexMetaData.actionMetaData

        return when {
            // start a new action
            stateMetaData?.name != stateName ->
                ActionMetaData(this.type, Instant.now().toEpochMilli(), this.actionIndex, false, 0, 0, null)
            actionMetaData?.index != this.actionIndex ->
                ActionMetaData(this.type, Instant.now().toEpochMilli(), this.actionIndex, false, 0, 0, null)
            // RetryAPI will reset startTime to null for actionMetaData and we'll reset it to "now" here
            else -> actionMetaData.copy(startTime = actionMetaData.startTime ?: Instant.now().toEpochMilli())
        }
    }

    /**
     * The implementer of Action can change this method to correctly serialize the internals of the action
     * when data is shared between nodes
     */
    open fun populateAction(out: StreamOutput) {
        out.writeInt(actionIndex)
    }

    /**
     * Get all the steps associated with the action
     */
    abstract fun getSteps(): List<Step>

    /**
     * Get the current step to execute in the action
     */
    abstract fun getStepToExecute(context: StepContext): Step

    final fun isLastStep(stepName: String): Boolean = getSteps().last().name == stepName

    final fun isFirstStep(stepName: String): Boolean = getSteps().first().name == stepName

    /*
     * Gets if the managedIndexMetaData reflects a state in which this action has completed successfully. Used in the
     * runner when determining if the index metadata should be deleted. If the action isFinishedSuccessfully and
     * deleteIndexMetadataAfterFinish is set to true, then we issue a request to delete the managedIndexConfig and its
     * managedIndexMetadata.
     */
    final fun isFinishedSuccessfully(managedIndexMetaData: ManagedIndexMetaData): Boolean {
        val policyRetryInfo = managedIndexMetaData.policyRetryInfo
        if (policyRetryInfo?.failed == true) return false
        val actionMetaData = managedIndexMetaData.actionMetaData
        if (actionMetaData == null || actionMetaData.failed || actionMetaData.name != this.type) return false
        val stepMetaData = managedIndexMetaData.stepMetaData
        if (stepMetaData == null || !isLastStep(stepMetaData.name) || stepMetaData.stepStatus != Step.StepStatus.COMPLETED) return false
        return true
    }

    /*
     * Denotes if the index metadata in the config index should be deleted for the index this action has just
     * successfully finished running on. This may be used by custom actions which delete some off-cluster index,
     * and following the action's success, the managed index config and metadata need to be deleted.
     */
    open fun deleteIndexMetadataAfterFinish(): Boolean = false

    companion object {
        const val DEFAULT_RETRIES = 3L
        const val CUSTOM_ACTION_FIELD = "custom"
        const val EXCLUDE_CUSTOM_FIELD_PARAM = "exclude_custom"
    }
}
