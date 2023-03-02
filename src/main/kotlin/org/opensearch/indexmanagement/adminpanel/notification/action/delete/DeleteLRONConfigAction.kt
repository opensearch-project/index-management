package org.opensearch.indexmanagement.adminpanel.notification.action.delete

import org.opensearch.action.ActionType
import org.opensearch.action.delete.DeleteResponse

class DeleteLRONConfigAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteLRONConfigAction()
        const val NAME = "cluster:admin/opendistro/adminpanel/lron/delete"
    }
}
