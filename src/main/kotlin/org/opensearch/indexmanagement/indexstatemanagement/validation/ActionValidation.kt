/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ValidationResult
import org.opensearch.monitor.jvm.JvmService

@OpenForTesting
class ActionValidation(
    val settings: Settings,
    val clusterService: ClusterService,
    val jvmService: JvmService
) {

    fun validate(actionName: String, indexName: String): ValidationResult {
        // map action to validation class
        val validation = when (actionName) {
            "rollover" -> ValidateRollover(settings, clusterService, jvmService).execute(indexName)
            "delete" -> ValidateDelete(settings, clusterService, jvmService).execute(indexName)
            "force_merge" -> ValidateForceMerge(settings, clusterService, jvmService).execute(indexName)
            "open" -> ValidateOpen(settings, clusterService, jvmService).execute(indexName)
            "read_only" -> ValidateReadOnly(settings, clusterService, jvmService).execute(indexName)
            "read_write" -> ValidateReadWrite(settings, clusterService, jvmService).execute(indexName)
            "replica_count" -> ValidateReplicaCount(settings, clusterService, jvmService).execute(indexName)
            else -> {
                // temporary call until all actions are mapped
                ValidateNothing(settings, clusterService, jvmService).execute(indexName)
            }
        }
        return ValidationResult(validation.validationMessage.toString(), validation.validationStatus)
    }
}
