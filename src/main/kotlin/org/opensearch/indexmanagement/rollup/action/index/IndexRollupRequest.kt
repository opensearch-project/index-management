/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.rollup.action.index

import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.ValidateActions.addValidationError
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.index.seqno.SequenceNumbers
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
