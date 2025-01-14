/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput

class DeleteSMPolicyRequest : DeleteRequest {
    override fun validate(): ActionRequestValidationException? = null

    constructor(sin: StreamInput) : super(sin)

    constructor(id: String) {
        super.id(id)
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
    }
}
