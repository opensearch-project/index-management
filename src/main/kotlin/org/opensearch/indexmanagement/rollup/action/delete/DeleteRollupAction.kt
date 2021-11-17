/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.delete

import org.opensearch.action.ActionType
import org.opensearch.action.delete.DeleteResponse

class DeleteRollupAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteRollupAction()
        const val NAME = "cluster:admin/opendistro/rollup/delete"
    }
}
