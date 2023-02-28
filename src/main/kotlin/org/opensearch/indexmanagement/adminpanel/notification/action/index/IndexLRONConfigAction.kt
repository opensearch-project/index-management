/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.index

import org.opensearch.action.ActionType

class IndexLRONConfigAction private constructor() :
    ActionType<IndexLRONConfigResponse>(NAME, ::IndexLRONConfigResponse) {
    companion object {
        val INSTANCE = IndexLRONConfigAction()
        const val NAME = "cluster:admin/opensearch/adminpanel/lron/write"
    }
}
