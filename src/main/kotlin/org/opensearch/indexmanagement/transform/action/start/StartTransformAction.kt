/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.start

import org.opensearch.action.ActionType
import org.opensearch.action.support.clustermanager.AcknowledgedResponse

class StartTransformAction private constructor() : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        val INSTANCE = StartTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/start"
    }
}
