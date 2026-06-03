/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate

import org.opensearch.action.ActionType

class SimulatePolicyAction private constructor() : ActionType<SimulatePolicyResponse>(NAME, ::SimulatePolicyResponse) {
    companion object {
        val INSTANCE = SimulatePolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/policy/simulate"
    }
}
