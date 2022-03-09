/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi

import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser
import org.opensearch.indexmanagement.spi.indexstatemanagement.DefaultStatusChecker
import org.opensearch.indexmanagement.spi.indexstatemanagement.IndexMetadataService
import org.opensearch.indexmanagement.spi.indexstatemanagement.StatusChecker

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
     * Status checker is used by IndexManagement to check the status of the extension before executing the actions registered by the extension.
     * Actions registered by the plugin can only be executed if in enabled, otherwise the action fails without retries. The status returned
     * should represent if the extension is enabled or disabled, and should not represent extension health or the availability of some extension
     * dependency.
     */
    fun statusChecker(): StatusChecker {
        return DefaultStatusChecker()
    }

    /**
     * Name of the extension
     */
    fun getExtensionName(): String

    /**
     * Not Required to override but if extension moves the index metadata outside of cluster state and requires IndexManagement to manage these
     * indices provide the metadata service that can provide the index metadata for these indices. An extension need to label the metadata service
     * with a type string which is used to distinguish indices in IndexManagement plugin
     */
    fun getIndexMetadataService(): Map<String, IndexMetadataService> {
        return mapOf()
    }

    /**
     * Caution: Experimental and can be removed in future
     *
     * If extension wants IndexManagement to determine cluster state indices UUID based on custom index setting if
     * present of cluster state override this method.
     */
    fun overrideClusterStateIndexUuidSetting(): String? {
        return null
    }
}
