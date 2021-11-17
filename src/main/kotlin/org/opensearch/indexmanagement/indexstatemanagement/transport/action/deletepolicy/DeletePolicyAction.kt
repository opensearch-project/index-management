/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy

import org.opensearch.action.ActionType
import org.opensearch.action.delete.DeleteResponse

class DeletePolicyAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeletePolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/policy/delete"
    }
}
