/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.monitor.jvm.JvmService
import java.util.Locale

abstract class Validate(
    val settings: Settings,
    val clusterService: ClusterService,
    val jvmService: JvmService
) {

    var validationStatus = ValidationStatus.PASSED
    var stepStatus = Step.StepStatus.STARTING
    var validationMessage: String? = "Starting Validation"

    abstract fun execute(indexName: String): Validate

    // function to be executed at policy creation to validate user created policy
    abstract fun validatePolicy(): Boolean

    abstract fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData, actionMetaData: ActionMetaData): ManagedIndexMetaData

    enum class ValidationStatus(val status: String) : Writeable {
        PASSED("passed"),
        RE_VALIDATING("re_validating"),
        FAILED("failed");

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
