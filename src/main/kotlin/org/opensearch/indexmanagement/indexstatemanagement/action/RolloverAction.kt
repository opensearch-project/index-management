/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class RolloverAction(
    val minSize: ByteSizeValue?,
    val minDocs: Long?,
    val minAge: TimeValue?,
    val minPrimaryShardSize: ByteSizeValue?,
    index: Int
) : Action(name, index) {

    init {
        if (minSize != null) require(minSize.bytes > 0) { "RolloverAction minSize value must be greater than 0" }

        if (minPrimaryShardSize != null) require(minPrimaryShardSize.bytes > 0) {
            "RolloverActionConfig minPrimaryShardSize value must be greater than 0"
        }
        if (minDocs != null) require(minDocs > 0) { "RolloverAction minDocs value must be greater than 0" }
    }

    private val attemptRolloverStep = AttemptRolloverStep(this)
    private val steps = listOf(attemptRolloverStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptRolloverStep
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        if (minSize != null) builder.field(MIN_SIZE_FIELD, minSize.stringRep)
        if (minDocs != null) builder.field(MIN_DOC_COUNT_FIELD, minDocs)
        if (minAge != null) builder.field(MIN_INDEX_AGE_FIELD, minAge.stringRep)
        if (minPrimaryShardSize != null) builder.field(MIN_PRIMARY_SHARD_SIZE_FIELD, minPrimaryShardSize.stringRep)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeOptionalWriteable(minSize)
        out.writeOptionalLong(minDocs)
        out.writeOptionalTimeValue(minAge)
        out.writeOptionalWriteable(minPrimaryShardSize)
        out.writeInt(actionIndex)
    }

    companion object {
        const val name = "rollover"
        const val MIN_SIZE_FIELD = "min_size"
        const val MIN_DOC_COUNT_FIELD = "min_doc_count"
        const val MIN_INDEX_AGE_FIELD = "min_index_age"
        const val MIN_PRIMARY_SHARD_SIZE_FIELD = "min_primary_shard_size"
    }
}
