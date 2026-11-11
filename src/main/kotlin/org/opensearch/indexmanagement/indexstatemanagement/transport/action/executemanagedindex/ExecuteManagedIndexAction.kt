/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.executemanagedindex

import org.opensearch.action.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class ExecuteManagedIndexAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = ExecuteManagedIndexAction()
        const val NAME = "cluster:admin/opendistro/ism/managedindex/execute"

        // Hop from the node that got the request to the node that owns the job. It is not reachable by users, only by other nodes, and
        // it does not check index permissions, so only the public action above may send to it.
        const val NODE_ACTION_NAME = "internal:cluster/opendistro/ism/managedindex/execute"
    }
}
