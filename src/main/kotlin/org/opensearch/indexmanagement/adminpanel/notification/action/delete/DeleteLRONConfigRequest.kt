/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.delete

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.util.NO_ID
import java.io.IOException

class DeleteLRONConfigRequest : ActionRequest {
    val docID: String
    val refreshPolicy: WriteRequest.RefreshPolicy

    constructor(
        docID: String = NO_ID,
        refreshPolicy: WriteRequest.RefreshPolicy
    ) : super() {
        this.docID = docID
        this.refreshPolicy = refreshPolicy
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        docID = sin.readString(),
        refreshPolicy = sin.readEnum(WriteRequest.RefreshPolicy::class.java)
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(docID)
        out.writeEnum(refreshPolicy)
    }
}
