/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.index

import org.opensearch.action.ActionType
import org.opensearch.indexmanagement.adminpanel.notification.LRONConfigResponse

class IndexLRONConfigAction private constructor() :
    ActionType<LRONConfigResponse>(NAME, ::LRONConfigResponse) {
    companion object {
        val INSTANCE = IndexLRONConfigAction()
        const val NAME = "cluster:admin/opensearch/adminpanel/lron/write"
    }
}
