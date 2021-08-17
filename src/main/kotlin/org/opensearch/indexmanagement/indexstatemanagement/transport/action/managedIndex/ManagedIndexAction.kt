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

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse

class ManagedIndexAction : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "indices:admin/opensearch/ism/managedindex"
        val INSTANCE = ManagedIndexAction()
    }
}
