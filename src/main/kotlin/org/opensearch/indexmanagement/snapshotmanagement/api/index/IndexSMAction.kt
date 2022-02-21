/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.index

import org.opensearch.action.ActionType

class IndexSMAction private constructor() : ActionType<IndexSMResponse>(NAME, ::IndexSMResponse) {
    companion object {
        val INSTANCE = IndexSMAction()
        const val NAME = "cluster:admin/opensearch/slm/put"
    }
}
