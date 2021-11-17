/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.index

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.ValidateActions.addValidationError
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.rollup.model.Rollup
import java.io.IOException

class IndexRollupRequest : IndexRequest {
    var rollup: Rollup

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        rollup = Rollup(sin)
        super.setRefreshPolicy(WriteRequest.RefreshPolicy.readFrom(sin))
    }

    constructor(
        rollup: Rollup,
        refreshPolicy: WriteRequest.RefreshPolicy
    ) {
        this.rollup = rollup
        if (rollup.seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || rollup.primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            this.opType(DocWriteRequest.OpType.CREATE)
        } else {
            this.setIfSeqNo(rollup.seqNo)
                .setIfPrimaryTerm(rollup.primaryTerm)
        }
        super.setRefreshPolicy(refreshPolicy)
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (rollup.id.isBlank()) {
            validationException = addValidationError("rollupID is missing", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        rollup.writeTo(out)
        refreshPolicy.writeTo(out)
    }
}
