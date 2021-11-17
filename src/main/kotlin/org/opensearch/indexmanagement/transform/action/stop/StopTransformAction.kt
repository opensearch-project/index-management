/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.stop

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse

class StopTransformAction private constructor() : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        val INSTANCE = StopTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/stop"
    }
}
