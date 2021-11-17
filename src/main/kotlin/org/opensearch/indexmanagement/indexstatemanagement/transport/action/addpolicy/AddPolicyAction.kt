/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy

import org.opensearch.action.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class AddPolicyAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = AddPolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/managedindex/add"
    }
}
