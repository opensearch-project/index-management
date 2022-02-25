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
     * Not Required to override but if extension moves the index metadata outside of cluster state and requires IndexManagement to manage these
     * indices provide the metadata service that can provide the index metadata for these indices. An extension need to label the metadata service
     * with a type string which is used to distinguish indices in IndexManagement plugin
     */
    fun getIndexMetadataService(): Map<String, IndexMetadataService> {
        return mapOf()
    }

    /**
     * Not required to override but if extension wants to evaluate the cluster events before deciding whether to auto manage indices
     * on index creation or should/not clean up managed indices when indices are deleted - add new handlers for the sepcific event type
     */
    fun getClusterEventHandlers(): List<ClusterEventHandler> {
        return listOf()
    }

    /**
     * Not Required to override but if extension wants IndexManagement to determine cluster state indices UUID based on custom index setting if
     * present of cluster state. This setting will be checked and used if a value is present otherwise fallback to default cluster state uuid
     */
    fun indexUUIDSetting(): String? {
        return null
    }
}
