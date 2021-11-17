/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.common.io.stream.Writeable

class UpdateManagedIndexMetaDataAction : ActionType<AcknowledgedResponse>(NAME, reader) {

    companion object {
        const val NAME = "cluster:admin/ism/update/managedindexmetadata"
        val INSTANCE = UpdateManagedIndexMetaDataAction()

        val reader = Writeable.Reader { AcknowledgedResponse(it) }
    }

    override fun getResponseReader(): Writeable.Reader<AcknowledgedResponse> = reader
}
