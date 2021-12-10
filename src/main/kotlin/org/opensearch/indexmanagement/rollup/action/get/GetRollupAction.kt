/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.get

import org.opensearch.action.ActionType

class GetRollupAction private constructor() : ActionType<GetRollupResponse>(NAME, ::GetRollupResponse) {
    companion object {
        val INSTANCE = GetRollupAction()
        const val NAME = "cluster:admin/opendistro/rollup/get"
    }
}
