/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.fielddomain

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.IndicesRequest
import org.opensearch.action.ValidateActions.addValidationError
import org.opensearch.action.support.IndicesOptions
import org.opensearch.core.common.Strings
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.index.Index
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainConfig

/** One concrete index and calculator configurations, never a caller-provided query or search source. */
class ComputeFieldDomainsRequest :
    ActionRequest,
    IndicesRequest.Replaceable {
    val targetIndex: Index
    val fields: List<FieldDomainConfig>

    constructor(targetIndex: Index, fields: List<FieldDomainConfig>) : super() {
        this.targetIndex = targetIndex
        this.fields = java.util.List.copyOf(fields)
    }

    constructor(sin: StreamInput) : super(sin) {
        targetIndex = Index(sin)
        fields = java.util.List.copyOf(sin.readList(::FieldDomainConfig))
    }

    override fun validate(): ActionRequestValidationException? {
        var failure: ActionRequestValidationException? = null
        if (targetIndex.name.isBlank()) failure = addValidationError("Target index name is required", failure)
        if (targetIndex.uuid.isBlank() || targetIndex.uuid == Strings.UNKNOWN_UUID_VALUE) {
            failure = addValidationError("Target index UUID is required", failure)
        }
        if (fields.isEmpty()) failure = addValidationError("Field configurations are required", failure)
        if (fields.map { it.field }.distinct().size != fields.size) {
            failure = addValidationError("Field configurations must have unique field names", failure)
        }
        return failure
    }

    override fun indices(): Array<String> = arrayOf(targetIndex.name)

    override fun indices(vararg indices: String): ComputeFieldDomainsRequest {
        // Security's index resolver requires Replaceable, but authorization must never retarget this operation.
        require(indices.contentEquals(arrayOf(targetIndex.name))) { "Field-domain computation requires the original concrete index" }
        return this
    }

    override fun indicesOptions(): IndicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed()

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        targetIndex.writeTo(out)
        out.writeList(fields)
    }
}
