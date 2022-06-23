/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.stop

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse

class StopRollupAction private constructor() : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        val INSTANCE = StopRollupAction()
        const val NAME = "cluster:admin/opendistro/rollup/stop"
    }
}
