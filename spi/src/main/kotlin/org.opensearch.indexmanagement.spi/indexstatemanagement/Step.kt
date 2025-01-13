/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement

import org.apache.logging.log4j.Logger
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import java.time.Instant
import java.util.Locale

abstract class Step(val name: String, val isSafeToDisableOn: Boolean = true) {
    var context: StepContext? = null
        private set

    fun preExecute(logger: Logger, context: StepContext): Step {
        logger.info("Executing $name for ${context.metadata.index}")
        this.context = context
        return this
    }

    abstract suspend fun execute(): Step

    fun postExecute(
        logger: Logger,
        indexManagementActionMetrics: IndexManagementActionsMetrics,
        step: Step,
        startingManagedIndexMetaData: ManagedIndexMetaData,
    ): Step {
        logger.info("Finished executing $name for ${context?.metadata?.index}")
        val updatedStepMetaData = step.getUpdatedManagedIndexMetadata(startingManagedIndexMetaData)
        emitTelemetry(indexManagementActionMetrics, updatedStepMetaData, logger)
        this.context = null
        return this
    }

    private fun emitTelemetry(
        indexManagementActionMetrics: IndexManagementActionsMetrics,
        updatedStepMetaData: ManagedIndexMetaData,
        logger: Logger,
    ) {
        when (context?.metadata?.actionMetaData?.name) {
            IndexManagementActionsMetrics.ROLLOVER -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.ROLLOVER,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.FORCE_MERGE -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.FORCE_MERGE,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.DELETE -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.DELETE,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.REPLICA_COUNT -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.REPLICA_COUNT,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.TRANSITION -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.TRANSITION,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.NOTIFICATION -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.NOTIFICATION,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.CLOSE -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.CLOSE,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.SET_INDEX_PRIORITY -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.SET_INDEX_PRIORITY, // problem in test
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.OPEN -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.OPEN,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.MOVE_SHARD -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.MOVE_SHARD,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.SET_READ_ONLY -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.SET_READ_ONLY,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.SHRINK -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.SHRINK,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.SNAPSHOT -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.SNAPSHOT,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.ALIAS_ACTION -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.ALIAS_ACTION,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            IndexManagementActionsMetrics.ALLOCATION -> indexManagementActionMetrics.getActionMetrics(
                IndexManagementActionsMetrics.ALLOCATION,
            )
                ?.emitMetrics(context!!, indexManagementActionMetrics, updatedStepMetaData.stepMetaData)

            else -> {
                logger.info(
                    "Action Metrics is not supported for this action [%s]",
                    context?.metadata?.actionMetaData?.name,
                )
            }
        }
    }

    abstract fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData

    abstract fun isIdempotent(): Boolean

    final fun getStepStartTime(metadata: ManagedIndexMetaData): Instant = when {
        metadata.stepMetaData == null -> Instant.now()
        metadata.stepMetaData.name != this.name -> Instant.now()
        // The managed index metadata is a historical snapshot of the metadata and refers to what has happened from the previous
        // execution, so if we ever see it as COMPLETED it means we are always going to be in a new step, this specifically
        // helps with the Transition -> Transition (empty state) sequence which the above do not capture
        metadata.stepMetaData.stepStatus == StepStatus.COMPLETED -> Instant.now()
        else -> Instant.ofEpochMilli(metadata.stepMetaData.startTime)
    }

    final fun getStartingStepMetaData(metadata: ManagedIndexMetaData): StepMetaData = StepMetaData(name, getStepStartTime(metadata).toEpochMilli(), StepStatus.STARTING)

    enum class StepStatus(val status: String) : Writeable {
        STARTING("starting"),
        CONDITION_NOT_MET("condition_not_met"),
        FAILED("failed"),
        COMPLETED("completed"),
        TIMED_OUT("timed_out"),
        ;

        override fun toString(): String = status

        override fun writeTo(out: StreamOutput) {
            out.writeString(status)
        }

        companion object {
            fun read(streamInput: StreamInput): StepStatus = valueOf(streamInput.readString().uppercase(Locale.ROOT))
        }
    }
}
