/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.mapping

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.rollup.model.Rollup

class UpdateRollupMappingRequest : AcknowledgedRequest<UpdateRollupMappingRequest> {
    val rollup: Rollup

    constructor(sin: StreamInput) : super(sin) {
        rollup = Rollup(sin)
    }

    constructor(rollup: Rollup) {
        this.rollup = rollup
    }

    override fun validate(): ActionRequestValidationException? = null

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        rollup.writeTo(out)
    }
}
