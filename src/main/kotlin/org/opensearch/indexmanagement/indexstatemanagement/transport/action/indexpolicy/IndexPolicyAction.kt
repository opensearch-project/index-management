/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy

import org.opensearch.action.ActionType

class IndexPolicyAction private constructor() : ActionType<IndexPolicyResponse>(NAME, ::IndexPolicyResponse) {
    companion object {
        val INSTANCE = IndexPolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/policy/write"
    }
}
