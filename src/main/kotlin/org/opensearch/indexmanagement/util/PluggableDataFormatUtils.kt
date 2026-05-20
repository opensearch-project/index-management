/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.util

import org.opensearch.cluster.metadata.Metadata

/**
 * Utility to check if indices have pluggable dataformat enabled.
 */
object PluggableDataFormatUtils {
    private const val PLUGGABLE_DATAFORMAT_ENABLED_KEY = "index.pluggable.dataformat.enabled"

    // TODO: Replace with IndexSettings.isPluggableDataFormatEnabled() once the OpenSearch core dependency is updated
    fun hasPluggableDataFormatEnabled(metadata: Metadata, concreteIndices: Array<String>): Boolean =
        concreteIndices.any { indexName ->
            val indexMetadata = metadata.index(indexName)
            indexMetadata != null &&
                indexMetadata.settings.getAsBoolean(PLUGGABLE_DATAFORMAT_ENABLED_KEY, false)
        }
}
