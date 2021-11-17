/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.refreshanalyzer

import org.opensearch.action.support.broadcast.BroadcastRequest
import org.opensearch.common.io.stream.StreamInput
import java.io.IOException

class RefreshSearchAnalyzerRequest : BroadcastRequest<RefreshSearchAnalyzerRequest> {
    @Suppress("SpreadOperator")
    constructor(vararg indices: String) : super(*indices)

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp)
}
