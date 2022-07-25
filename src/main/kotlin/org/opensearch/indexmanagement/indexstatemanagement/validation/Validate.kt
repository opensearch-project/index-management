/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.util.OpenForTesting
import java.util.*

@OpenForTesting
abstract class Validate(
    val settings: Settings,
    val clusterService: ClusterService // what settings do I need, make it private?
) {

    var validationStatus = ValidationStatus.PASS
    var stepStatus = Step.StepStatus.STARTING

    abstract fun executeValidation(context: StepContext): Validate

    abstract fun validatePolicy(): Boolean

    // abstract fun validateGeneric(): Boolean

    enum class ValidationStatus(val status: String) : Writeable {
        PASS("pass"),
        REVALIDATE("revalidate"),
        FAIL("fail");

        override fun toString(): String {
            return status
        }

        override fun writeTo(out: StreamOutput) {
            out.writeString(status)
        }

        companion object {
            fun read(streamInput: StreamInput): ValidationStatus {
                return valueOf(streamInput.readString().uppercase(Locale.ROOT))
            }
        }
    }
}
