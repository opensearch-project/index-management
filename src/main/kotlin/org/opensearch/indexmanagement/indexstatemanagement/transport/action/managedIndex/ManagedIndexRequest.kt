/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex

import org.opensearch.action.support.broadcast.BroadcastRequest
import org.opensearch.common.io.stream.StreamInput
import java.io.IOException

@Suppress("SpreadOperator")
class ManagedIndexRequest : BroadcastRequest<ManagedIndexRequest> {

    constructor(vararg indices: String) : super(*indices)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin)
}
