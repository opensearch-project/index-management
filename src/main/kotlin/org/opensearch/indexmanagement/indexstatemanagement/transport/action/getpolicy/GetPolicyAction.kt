/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.opensearch.action.ActionType

class GetPolicyAction private constructor() : ActionType<GetPolicyResponse>(NAME, ::GetPolicyResponse) {
    companion object {
        val INSTANCE = GetPolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/policy/get"
    }
}
