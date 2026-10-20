/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.fielddomain

import org.opensearch.action.ActionType

/**
 * Authorizes computation of index-wide summaries, including values hidden from the caller by DLS, FLS, or masking.
 * Grant this index permission only to identities trusted to obtain those summaries. It does not authorize publication.
 */
class ComputeFieldDomainsAction private constructor() : ActionType<ComputeFieldDomainsResponse>(NAME, ::ComputeFieldDomainsResponse) {
    companion object {
        val INSTANCE = ComputeFieldDomainsAction()
        const val NAME = "indices:admin/opensearch/ism/field_domains/compute"
    }
}
