/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.indexmanagement.transform.settings.TransformSettings
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.monitor.jvm.JvmService

@OpenForTesting
class ValidateReplicaCount(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService
) : Validate(settings, clusterService, jvmService) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("ReturnSuppressCount", "ReturnCount")
    override fun execute(indexName: String): Validate {
        // if these conditions are false, fail validation and do not set replica count to the index
        if (dataSizeTooLarge(indexName)) {
            return this
        }
        validationMessage = getValidationPassedMessage(indexName)
        return this
    }

    // TODO: evaluate incoming shard number changes dynamically
    fun dataSizeTooLarge(indexName: String): Boolean {
        val circuitBreakerEnabled = TransformSettings.TRANSFORM_CIRCUIT_BREAKER_ENABLED.get(settings)
        val circuitBreakerJvmThreshold = TransformSettings.TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD.get(settings)
        if (circuitBreakerEnabled && jvmService.stats().mem.heapUsedPercent > circuitBreakerJvmThreshold) {
            val message = getFailedDataTooLargeMessage(indexName)
            logger.warn(message)
            validationStatus = ValidationStatus.RE_VALIDATING
            return true
        }
        return false
    }

    fun maxNumberOfShardsExceeded(indexName: String): Boolean {
        val totalShards = clusterService.state().metadata.totalNumberOfShards
        val openShards = clusterService.state().metadata.totalOpenIndexShards
        val numberOfShards = clusterService.state().metadata.index(indexName).numberOfShards
        val replicaCount = clusterService.state().metadata.index(indexName).numberOfReplicas
        if (replicaCount * numberOfShards > (totalShards - openShards)) {
            val message = getMaxNumberOfShardsExceededMessage(indexName)
            logger.warn(message)
            validationStatus = ValidationStatus.RE_VALIDATING
            return true
        }
        return false
    }

    @Suppress("TooManyFunctions")
    companion object {
        const val name = "validate_replica_count"
        fun getFailedDataTooLargeMessage(index: String) = "Data too large and is over the allowed limit for index [index=$index]"
        fun getMaxNumberOfShardsExceededMessage(index: String) = "Maximum number of shards exceeded for index [index=$index]"
        fun getValidationPassedMessage(index: String) = "Replica Count validation passed for [index=$index]"
    }
}
