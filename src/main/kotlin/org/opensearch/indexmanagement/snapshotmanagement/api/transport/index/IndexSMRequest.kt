/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.index

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy

class IndexSMRequest(val smPolicy: SMPolicy) : ActionRequest() {
    override fun validate(): ActionRequestValidationException? {
        return null
    }

    constructor(sin: StreamInput) : this(
        smPolicy = SMPolicy(sin)
    )

    override fun writeTo(out: StreamOutput) {
        smPolicy.writeTo(out)
    }
}
