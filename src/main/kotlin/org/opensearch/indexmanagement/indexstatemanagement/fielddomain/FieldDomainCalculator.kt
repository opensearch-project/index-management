/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.fielddomain

import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.index.fielddomain.FieldDomain
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

/**
 * Computes one type of field-domain metadata for a managed concrete index.
 */
interface FieldDomainCalculator {
    val type: String

    /**
     * Computes a field domain or returns null when the field has no values in the index.
     */
    suspend fun calculate(
        context: StepContext,
        indexMetadata: IndexMetadata,
        config: FieldDomainConfig,
        finalized: Boolean,
    ): FieldDomain?
}
