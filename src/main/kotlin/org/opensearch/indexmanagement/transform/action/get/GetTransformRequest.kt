/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.get

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetTransformRequest(
    val id: String,
    val srcContext: FetchSourceContext? = null,
    val preference: String? = null
) : ActionRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        srcContext = if (sin.readBoolean()) FetchSourceContext(sin) else null,
        preference = sin.readOptionalString()
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (id.isBlank()) {
            validationException = ValidateActions.addValidationError("id is missing", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        if (srcContext == null) {
            out.writeBoolean(false)
        } else {
            out.writeBoolean(true)
            srcContext.writeTo(out)
        }
        out.writeOptionalString(preference)
    }
}
