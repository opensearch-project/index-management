/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.index

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.ValidateActions.addValidationError
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.transform.model.Transform
import java.io.IOException

class IndexTransformRequest : IndexRequest {
    var transform: Transform

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        transform = Transform(sin)
        super.setRefreshPolicy(WriteRequest.RefreshPolicy.readFrom(sin))
    }

    constructor(
        transform: Transform,
        refreshPolicy: WriteRequest.RefreshPolicy
    ) {
        this.transform = transform
        if (transform.seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || transform.primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            this.opType(DocWriteRequest.OpType.CREATE)
        } else {
            this.setIfSeqNo(transform.seqNo)
                .setIfPrimaryTerm(transform.primaryTerm)
        }
        super.setRefreshPolicy(refreshPolicy)
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (transform.id.isBlank()) {
            validationException = addValidationError("transformID is missing", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        transform.writeTo(out)
        refreshPolicy.writeTo(out)
    }
}
