/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.get

import org.opensearch.action.ActionType
import org.opensearch.indexmanagement.controlcenter.notification.LRONConfigResponse

class GetLRONConfigAction private constructor() : ActionType<LRONConfigResponse>(NAME, ::LRONConfigResponse) {
    companion object {
        val INSTANCE = GetLRONConfigAction()
        const val NAME = "cluster:admin/opensearch/controlcenter/lron/get"
    }
}
