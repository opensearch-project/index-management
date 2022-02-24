/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import org.apache.logging.log4j.Logger
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import java.time.Instant
import java.util.Locale

abstract class Step(val name: String, val managedIndexMetaData: ManagedIndexMetaData, val isSafeToDisableOn: Boolean = true) {

    fun preExecute(logger: Logger): Step {
        logger.info("Executing $name for ${managedIndexMetaData.index}")
        return this
    }

    abstract suspend fun execute(): Step

    fun postExecute(logger: Logger): Step {
        logger.info("Finished executing $name for ${managedIndexMetaData.index}")
        return this
    }

    abstract fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData

    /**
     * Before every execution of a step, we first update the step_status in cluster state to [StepStatus.STARTING]
     * to signal that work is about to be done for the managed index. The step then attempts to do work by
     * calling execute, and finally updates the step_status with the results of that work ([StepStatus]).
     *
     * If we ever start an execution with a step_status of [StepStatus.STARTING] it means we failed to update the step_status
     * after calling the execute function. Since we do not know if the execution was a noop, failed, or completed then
     * we can't always assume it's safe to just retry it (e.g. calling force merge multiple times in a row). This means
     * that final update is a failure point that can't be retried and when multiplied by # of executions it leads to a lot of
     * chances over time for random network failures, timeouts, etc.
     *
     * To get around this every step should have an [isIdempotent] method to signal if it's safe to retry this step for such failures.
     */
    abstract fun isIdempotent(): Boolean

    fun getStartingStepMetaData(): StepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), StepStatus.STARTING)

    fun getStepStartTime(): Instant {
        return when {
            managedIndexMetaData.stepMetaData == null -> Instant.now()
            managedIndexMetaData.stepMetaData.name != this.name -> Instant.now()
            // The managed index metadata is a historical snapshot of the metadata and refers to what has happened from the previous
            // execution, so if we ever see it as COMPLETED it means we are always going to be in a new step, this specifically
            // helps with the Transition -> Transition (empty state) sequence which the above do not capture
            managedIndexMetaData.stepMetaData.stepStatus == StepStatus.COMPLETED -> Instant.now()
            else -> Instant.ofEpochMilli(managedIndexMetaData.stepMetaData.startTime)
        }
    }

    protected val indexName: String = managedIndexMetaData.index
    protected val indexCreationDate: Long? = managedIndexMetaData.indexCreationDate

    enum class StepStatus(val status: String) : Writeable {
        STARTING("starting"),
        CONDITION_NOT_MET("condition_not_met"),
        FAILED("failed"),
        COMPLETED("completed");

        override fun toString(): String {
            return status
        }

        override fun writeTo(out: StreamOutput) {
            out.writeString(status)
        }

        companion object {
            fun read(streamInput: StreamInput): StepStatus {
                return valueOf(streamInput.readString().toUpperCase(Locale.ROOT))
            }
        }
    }
}
