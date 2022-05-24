/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.execute

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput

class ExecuteSMRequest(val snapshotPolicyID: String) : ActionRequest() {

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (this.snapshotPolicyID.isBlank()) {
            validationException = ValidateActions.addValidationError("id is missing", validationException)
        }
        return validationException
    }

    constructor(sin: StreamInput) : this(snapshotPolicyID = sin.readString())

    override fun writeTo(out: StreamOutput) { out.writeString(snapshotPolicyID) }
}
