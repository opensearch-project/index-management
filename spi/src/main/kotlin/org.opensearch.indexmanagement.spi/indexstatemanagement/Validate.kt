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
import java.util.Locale

abstract class Validate(
    val settings: Settings,
    val clusterService: ClusterService
) {

    var validationStatus = ValidationStatus.PASS
    var stepStatus = Step.StepStatus.STARTING
    var validationMessage: String? = "Starting Validation"

    abstract fun executeValidation(indexName: String): Validate

    abstract fun validatePolicy(): Boolean

    abstract fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData, actionMetaData: ActionMetaData): ManagedIndexMetaData

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
