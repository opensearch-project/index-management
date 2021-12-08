/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi

import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser
import org.opensearch.indexmanagement.spi.indexstatemanagement.ClusterEventHandler
import org.opensearch.indexmanagement.spi.indexstatemanagement.IndexMetadataService

/**
 * SPI for IndexManagement
 */
interface IndexManagementExtension {

    /**
     * List of action parsers that are supported by the extension, each of the action parser will parse the policy action into the defined action.
     * The ActionParser provides the ability to parse the action
     */
    fun getISMActionParsers(): List<ActionParser>

    /**
     * Not Required to override but if extension is introducing a new index type and special handling is needed to handle this type
     * use this to provide the metadata service for the new index types
     */
    fun getIndexMetadataService(): Map<String, IndexMetadataService> {
        return mapOf()
    }

    /**
     * Not required to override but if extension wants to evaluate the cluster events before deciding whether to auto manage indices
     * on index creation or should/not clean up managed indices when indices are deleted - add new handlers for the sepcific event type
     */
    fun getClusterEventHandlers(): Map<ClusterEventType, ClusterEventHandler> {
        return mapOf()
    }
}

enum class ClusterEventType(val type: String) {
    CREATE("create"),
    DELETE("delete");

    override fun toString(): String {
        return type
    }
}
