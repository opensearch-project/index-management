/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse

class ManagedIndexAction : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "indices:admin/opensearch/ism/managedindex"
        val INSTANCE = ManagedIndexAction()
    }
}
