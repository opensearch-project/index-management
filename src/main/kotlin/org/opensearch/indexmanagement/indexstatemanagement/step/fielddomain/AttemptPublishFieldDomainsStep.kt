/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.fielddomain

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.fielddomain.PutIndexFieldDomainsAction
import org.opensearch.action.admin.indices.fielddomain.PutIndexFieldDomainsRequest
import org.opensearch.action.admin.indices.refresh.RefreshRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE
import org.opensearch.index.fielddomain.FieldDomain
import org.opensearch.indexmanagement.indexstatemanagement.action.PublishFieldDomainsAction
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainCalculatorRegistry
import org.opensearch.indexmanagement.indexstatemanagement.util.isIndexWriteBlocked
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException

/**
 * Computes finalized field domains and publishes them through the core field-domain metadata action.
 *
 * The step requires the managed index to be write-blocked, then refreshes it before computing domains. This makes
 * the published metadata safe for consumers that use finalized field domains for search-time pruning.
 */
class AttemptPublishFieldDomainsStep(
    private val action: PublishFieldDomainsAction,
    private val calculatorRegistry: FieldDomainCalculatorRegistry = FieldDomainCalculatorRegistry.defaultRegistry(),
) : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        val context = this.context
        if (context != null) {
            val indexName = context.metadata.index
            try {
                attemptPublish(context, indexName)
            } catch (e: RemoteTransportException) {
                handleException(indexName, ExceptionsHelper.unwrapCause(e))
            } catch (e: Exception) {
                handleException(indexName, e)
            }
        }

        return this
    }

    private suspend fun attemptPublish(context: StepContext, indexName: String) {
        val indexMetadata = currentIndexMetadata(context, indexName)
        if (indexMetadata != null && validateWriteBlock(indexMetadata)) {
            refreshIndex(context, indexName)
            val domains = calculateDomains(context, indexMetadata)
            if (domains.isEmpty()) {
                markNoDomains(indexName)
            } else {
                publishDomains(context, indexMetadata, domains)
            }
        }
    }

    private fun currentIndexMetadata(context: StepContext, indexName: String): IndexMetadata? {
        val indexMetadata = context.clusterService.state().metadata.index(indexName)
        return when {
            indexMetadata == null -> {
                val message = getIndexMissingMessage(indexName)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
                null
            }

            indexMetadata.indexUUID != context.metadata.indexUuid -> {
                val message = getIndexUuidMismatchMessage(indexName)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = mapOf(
                    "message" to message,
                    "expected_uuid" to context.metadata.indexUuid,
                    "actual_uuid" to indexMetadata.indexUUID,
                )
                null
            }

            else -> indexMetadata
        }
    }

    private suspend fun calculateDomains(context: StepContext, indexMetadata: IndexMetadata): List<FieldDomain> =
        action.fields.mapNotNull { fieldConfig ->
            val calculator = calculatorRegistry.get(fieldConfig.type)
                ?: throw IllegalArgumentException("No field-domain calculator registered for type [${fieldConfig.type}]")
            calculator.calculate(context, indexMetadata, fieldConfig, FINALIZED)
        }

    private fun validateWriteBlock(indexMetadata: IndexMetadata): Boolean {
        val indexName = indexMetadata.index.name
        if (isIndexWriteBlocked(indexMetadata)) {
            return true
        }

        val message = getWriteBlockRequiredMessage(indexName)
        logger.info(message)
        stepStatus = StepStatus.CONDITION_NOT_MET
        info = mapOf("message" to message)
        return false
    }

    private suspend fun refreshIndex(context: StepContext, indexName: String) {
        val response = context.client.admin().indices().suspendUntil {
            refresh(RefreshRequest(indexName), it)
        }
        if (response.failedShards > 0) {
            throw IllegalStateException(
                "Failed to refresh index [$indexName] before publishing field domains, " +
                    "shard failures [${response.shardFailures.joinToString { it.toString() }}]",
            )
        }
    }

    private suspend fun publishDomains(context: StepContext, indexMetadata: IndexMetadata, domains: List<FieldDomain>) {
        val request = PutIndexFieldDomainsRequest(indexMetadata.index).fieldDomains(domains)
        val response: AcknowledgedResponse = context.client.suspendUntil {
            execute(PutIndexFieldDomainsAction.INSTANCE, request, it)
        }

        if (response.isAcknowledged) {
            stepStatus = StepStatus.COMPLETED
            info = mapOf(
                "message" to getSuccessMessage(indexMetadata.index.name, domains.size),
                "fields" to domains.map { it.field() },
            )
        } else {
            val message = getFailedPublishMessage(indexMetadata.index.name)
            logger.warn(message)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message)
        }
    }

    private fun markNoDomains(indexName: String) {
        stepStatus = StepStatus.COMPLETED
        info = mapOf("message" to getNoDomainMessage(indexName))
    }

    private fun handleException(indexName: String, e: Throwable) {
        val message = getFailedPublishMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf<String, Any>("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData = currentMetadata.copy(
        stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
        transitionTo = null,
        info = info,
    )

    override fun isIdempotent(): Boolean = true

    companion object {
        const val name = "attempt_publish_field_domains"
        private const val FINALIZED = true

        fun getIndexMissingMessage(index: String) = "Cannot publish field domains because index is missing [index=$index]"

        fun getIndexUuidMismatchMessage(index: String) =
            "Cannot publish field domains because managed index UUID does not match current index metadata [index=$index]"

        fun getWriteBlockRequiredMessage(index: String) =
            "Cannot publish finalized field domains because index write block is not enabled [index=$index]; " +
                "add a preceding read_only action or enable [$SETTING_BLOCKS_WRITE] before this action"

        fun getNoDomainMessage(index: String) = "No field domains to publish [index=$index]"

        fun getFailedPublishMessage(index: String) = "Failed to publish field domains [index=$index]"

        fun getSuccessMessage(index: String, domainCount: Int) =
            "Successfully published [$domainCount] field domain(s) [index=$index]"
    }
}
