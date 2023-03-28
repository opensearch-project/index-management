/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.get

import org.opensearch.action.ActionType

class GetLRONConfigAction private constructor() : ActionType<GetLRONConfigResponse>(NAME, ::GetLRONConfigResponse) {
    companion object {
        val INSTANCE = GetLRONConfigAction()
        const val NAME = "cluster:admin/opensearch/controlcenter/lron/get"
    }
}
