/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.transform.action.delete

import org.opensearch.action.ActionType
import org.opensearch.action.bulk.BulkResponse

class DeleteTransformsAction private constructor() : ActionType<BulkResponse>(NAME, ::BulkResponse) {
    companion object {
        val INSTANCE = DeleteTransformsAction()
        val NAME = "cluster:admin/opendistro/transform/delete"
    }
}
