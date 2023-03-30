/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.delete

import org.opensearch.action.ActionType
import org.opensearch.action.delete.DeleteResponse

class DeleteLRONConfigAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteLRONConfigAction()
        const val NAME = "cluster:admin/opendistro/controlcenter/lron/delete"
    }
}
