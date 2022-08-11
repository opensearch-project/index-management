/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.executepolicy

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse

class ExecutePolicyAction private constructor() : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        val INSTANCE = ExecutePolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/managedindex/execute"
    }
}
