/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.explain

import org.opensearch.action.ActionType

class ExplainTransformAction private constructor() : ActionType<ExplainTransformResponse>(NAME, ::ExplainTransformResponse) {
    companion object {
        val INSTANCE = ExplainTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/explain"
    }
}
