/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.get

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.common.model.rest.SearchParams
import org.opensearch.indexmanagement.controlcenter.notification.util.LRON_DOC_ID_PREFIX
import java.io.IOException

class GetLRONConfigRequest(
    val docId: String? = null,
    val searchParams: SearchParams? = null
) : ActionRequest() {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        docId = sin.readOptionalString(),
        searchParams = sin.readOptionalWriteable(::SearchParams)
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (null == docId && null == searchParams) {
            validationException = ValidateActions.addValidationError(
                "GetLRONConfigRequest must contain docId or searchParams",
                validationException
            )
        }
        if (null != docId && null != searchParams) {
            validationException = ValidateActions.addValidationError(
                "GetLRONConfigRequest can not contain both docId and searchParams",
                validationException
            )
        }
        if (null != docId && !docId.startsWith(LRON_DOC_ID_PREFIX)) {
            validationException = ValidateActions.addValidationError(
                "Invalid LRONConfig ID",
                validationException
            )
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(docId)
        out.writeOptionalWriteable(searchParams)
    }
}
