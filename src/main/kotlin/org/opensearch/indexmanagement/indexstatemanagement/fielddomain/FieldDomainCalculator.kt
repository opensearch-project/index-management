/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.fielddomain

import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.index.fielddomain.FieldDomain

/**
 * Computes one type of field-domain metadata for a managed concrete index.
 */
interface FieldDomainCalculator {
    val type: String

    /**
     * Computes a field domain or returns null when the field has no values in the index.
     * All index reads must use [context], which provides index-wide search access independently of the policy user's view.
     */
    suspend fun calculate(
        context: FieldDomainSearchContext,
        indexMetadata: IndexMetadata,
        config: FieldDomainConfig,
        finalized: Boolean,
    ): FieldDomain?
}
