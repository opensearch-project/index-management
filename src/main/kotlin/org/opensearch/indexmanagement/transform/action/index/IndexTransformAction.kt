/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.index

import org.opensearch.action.ActionType

class IndexTransformAction private constructor() : ActionType<IndexTransformResponse>(NAME, ::IndexTransformResponse) {
    companion object {
        val INSTANCE = IndexTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/index"
    }
}
