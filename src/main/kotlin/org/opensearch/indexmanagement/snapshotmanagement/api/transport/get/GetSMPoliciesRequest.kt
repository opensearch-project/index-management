/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.get

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput

class GetSMPoliciesRequest() : ActionRequest() {
    override fun validate(): ActionRequestValidationException? {
        return null
    }

    constructor(sin: StreamInput) : this()
}
