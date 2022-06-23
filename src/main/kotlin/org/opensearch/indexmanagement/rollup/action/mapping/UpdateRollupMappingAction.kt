/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.mapping

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.common.io.stream.Writeable

class UpdateRollupMappingAction : ActionType<AcknowledgedResponse>(NAME, reader) {

    companion object {
        const val NAME = "cluster:admin/opendistro/rollup/mapping/update"
        val INSTANCE = UpdateRollupMappingAction()
        val reader = Writeable.Reader { AcknowledgedResponse(it) }
    }

    override fun getResponseReader(): Writeable.Reader<AcknowledgedResponse> = reader
}
