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
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

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

    companion object {
        const val DEFAULT_RETRIES = 3L
        const val CUSTOM_ACTION_FIELD = "custom"
        const val EXCLUDE_CUSTOM_FIELD_PARAM = "exclude_custom"
    }
}
