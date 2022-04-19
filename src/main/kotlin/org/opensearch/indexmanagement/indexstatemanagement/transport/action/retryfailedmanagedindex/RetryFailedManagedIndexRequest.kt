/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import java.io.IOException

class RetryFailedManagedIndexRequest(
    val indices: List<String>,
    val startState: String?,
    val clusterManagerTimeout: TimeValue,
    val indexType: String
) : ActionRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indices = sin.readStringList(),
        startState = sin.readOptionalString(),
        clusterManagerTimeout = sin.readTimeValue(),
        indexType = sin.readString()
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (indices.isEmpty()) {
            validationException = ValidateActions.addValidationError("Missing indices", validationException)
        } else if (indexType != DEFAULT_INDEX_TYPE && indices.size > 1) {
            validationException = ValidateActions.addValidationError(
                MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR,
                validationException
            )
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indices)
        out.writeOptionalString(startState)
        out.writeTimeValue(clusterManagerTimeout)
        out.writeString(indexType)
    }

    companion object {
        const val MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR =
            "Cannot retry on more than one index name/pattern when using a custom index type"
    }
}
