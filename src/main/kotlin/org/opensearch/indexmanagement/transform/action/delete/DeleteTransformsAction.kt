/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.delete

import org.opensearch.action.ActionType
import org.opensearch.action.bulk.BulkResponse

class DeleteTransformsAction private constructor() : ActionType<BulkResponse>(NAME, ::BulkResponse) {
    companion object {
        val INSTANCE = DeleteTransformsAction()
        const val NAME = "cluster:admin/opendistro/transform/delete"
    }
}
