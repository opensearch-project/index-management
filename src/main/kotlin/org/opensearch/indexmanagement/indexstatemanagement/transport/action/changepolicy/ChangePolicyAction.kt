/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import org.opensearch.action.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class ChangePolicyAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = ChangePolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/managedindex/change"
    }
}
