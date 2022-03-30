/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptShrinkStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForShrinkStep
import org.opensearch.indexmanagement.opensearchapi.aliasesField
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

@Suppress("LongParameterList")
class ShrinkAction(
    val numNewShards: Int?,
    val maxShardSize: ByteSizeValue?,
    val percentageDecrease: Double?,
    val targetIndexSuffix: String?,
    val aliases: List<Alias>?,
    val forceUnsafe: Boolean?,
    index: Int
) : Action(name, index) {
    init {
        val numSet = arrayOf(maxShardSize != null, percentageDecrease != null, numNewShards != null).count { it }
        require(numSet == 1) { "Exactly one option specifying the number of shards to shrink to must be used." }

        if (maxShardSize != null) {
            require(maxShardSize.bytes > 0) { "Shrink action maxShardSize must be greater than 0." }
        } else if (percentageDecrease != null) {
            require(percentageDecrease > 0.0 && percentageDecrease < 1.0) { "Percentage decrease must be between 0.0 and 1.0 exclusively" }
        } else if (numNewShards != null) {
            require(numNewShards > 0) { "Shrink action numNewShards must be greater than 0." }
        }
    }

    private val attemptMoveShardsStep = AttemptMoveShardsStep(this)
    private val waitForMoveShardsStep = WaitForMoveShardsStep(this)
    private val attemptShrinkStep = AttemptShrinkStep(this)
    private val waitForShrinkStep = WaitForShrinkStep(this)

    private val stepNameToStep: LinkedHashMap<String, Step> = linkedMapOf(
        AttemptMoveShardsStep.name to attemptMoveShardsStep,
        WaitForMoveShardsStep.name to waitForMoveShardsStep,
        AttemptShrinkStep.name to attemptShrinkStep,
        WaitForShrinkStep.name to waitForShrinkStep
    )
    override fun getSteps(): List<Step> = listOf(attemptMoveShardsStep, waitForMoveShardsStep, attemptShrinkStep, waitForShrinkStep)

    @SuppressWarnings("ReturnCount")
    override fun getStepToExecute(context: StepContext): Step {
        val stepMetaData = context.metadata.stepMetaData ?: return attemptMoveShardsStep
        val currentStep = stepMetaData.name

        // If the current step is not from this action, assume it is from another action.
        if (!stepNameToStep.containsKey(currentStep)) return attemptMoveShardsStep

        val currentStepStatus = stepMetaData.stepStatus
        if (currentStepStatus == Step.StepStatus.COMPLETED) {
            return when (currentStep) {
                AttemptMoveShardsStep.name -> waitForMoveShardsStep
                WaitForMoveShardsStep.name -> attemptShrinkStep
                AttemptShrinkStep.name -> waitForShrinkStep
                else -> stepNameToStep[currentStep]!!
            }
        }
        // step not completed
        return stepNameToStep[currentStep]!!
    }

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        if (numNewShards != null) builder.field(NUM_NEW_SHARDS_FIELD, numNewShards)
        if (maxShardSize != null) builder.field(MAX_SHARD_SIZE_FIELD, maxShardSize.stringRep)
        if (percentageDecrease != null) builder.field(PERCENTAGE_DECREASE_FIELD, percentageDecrease)
        if (targetIndexSuffix != null) builder.field(TARGET_INDEX_SUFFIX_FIELD, targetIndexSuffix)
        if (aliases != null) { builder.aliasesField(aliases) }
        if (forceUnsafe != null) builder.field(FORCE_UNSAFE_FIELD, forceUnsafe)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeOptionalInt(numNewShards)
        out.writeOptionalWriteable(maxShardSize)
        out.writeOptionalDouble(percentageDecrease)
        out.writeOptionalString(targetIndexSuffix)
        if (aliases != null) {
            out.writeBoolean(true)
            out.writeList(aliases)
        } else {
            out.writeBoolean(false)
        }
        out.writeOptionalBoolean(forceUnsafe)
        out.writeInt(actionIndex)
    }

    companion object {
        const val name = "shrink"
        const val NUM_NEW_SHARDS_FIELD = "num_new_shards"
        const val PERCENTAGE_DECREASE_FIELD = "percentage_decrease"
        const val MAX_SHARD_SIZE_FIELD = "max_shard_size"
        const val TARGET_INDEX_SUFFIX_FIELD = "target_index_suffix"
        const val ALIASES_FIELD = "aliases"
        const val FORCE_UNSAFE_FIELD = "force_unsafe"
    }
}
