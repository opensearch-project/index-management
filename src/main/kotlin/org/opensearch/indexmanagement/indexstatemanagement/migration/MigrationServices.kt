/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.migration

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse
import org.opensearch.action.admin.indices.template.post.SimulateIndexTemplateResponse
import org.opensearch.action.admin.indices.template.post.SimulateTemplateAction
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkItemResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.update.UpdateRequest
import org.opensearch.action.update.UpdateResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.Template
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.updateISMTemplateRequest
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.rest.RestStatus
import java.time.Instant

@OpenForTesting
@Suppress("MagicNumber", "ReturnCount", "ThrowsCount", "TooManyFunctions", "ComplexMethod", "NestedBlockDepth")
class ISMTemplateService(
    private val client: Client,
    private val clusterService: ClusterService,
    private val xContentRegistry: NamedXContentRegistry,
    private val imIndices: IndexManagementIndices
) {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile final var finishFlag = false
        private set
    fun reenableTemplateMigration() { finishFlag = false }

    @Volatile var runTimeCounter = 0

    private var ismTemplateMap = mutableMapOf<policyID, MutableList<ISMTemplate>>()
    private val v1TemplatesWithPolicyID = mutableMapOf<templateName, V1TemplateCache>()

    private val negOrderToPositive = mutableMapOf<Int, Int>()
    private val v1orderToTemplatesName = mutableMapOf<Int, MutableList<templateName>>()
    private val v1orderToBucketIncrement = mutableMapOf<Int, Int>()

    private val policiesToUpdate = mutableMapOf<policyID, seqNoPrimaryTerm>()
    private val policiesFailedToUpdate = mutableMapOf<policyID, BulkItemResponse.Failure>()
    private lateinit var lastUpdatedTime: Instant

    @Volatile private var retryPolicy =
        BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(50), 3)

    suspend fun doMigration(timeStamp: Instant) {
        if (runTimeCounter >= 10) {
            stopMigration(-2)
            return
        }
        logger.info("Doing ISM template migration ${++runTimeCounter} time.")
        cleanCache()

        lastUpdatedTime = timeStamp.minusSeconds(3600)
        logger.info("Use $lastUpdatedTime as migrating ISM template last_updated_time")

        getIndexTemplates()
        logger.info("ISM templates: $ismTemplateMap")

        getISMPolicies()
        logger.info("Policies to update: ${policiesToUpdate.keys}")

        updateISMPolicies()

        if (policiesToUpdate.isEmpty()) {
            stopMigration(-1)
        }
    }

    private fun stopMigration(successFlag: Long) {
        finishFlag = true
        val newSetting = Settings.builder().put(ManagedIndexSettings.TEMPLATE_MIGRATION_CONTROL.key, successFlag)
        val request = ClusterUpdateSettingsRequest().persistentSettings(newSetting)
        client.admin().cluster().updateSettings(request, updateSettingListener())
        logger.info("Failure experienced when migrating ISM Template and update ISM policies: $policiesFailedToUpdate")
        // TODO what if update setting failed, cannot reset to -1/-2
        runTimeCounter = 0
    }

    private fun updateSettingListener(): ActionListener<ClusterUpdateSettingsResponse> {
        return object : ActionListener<ClusterUpdateSettingsResponse> {
            override fun onFailure(e: Exception) {
                logger.error("Failed to update template migration setting", e)
            }

            override fun onResponse(response: ClusterUpdateSettingsResponse) {
                if (!response.isAcknowledged) {
                    logger.error("Update template migration setting is not acknowledged")
                } else {
                    logger.info("Successfully update template migration setting")
                }
            }
        }
    }

    private suspend fun getIndexTemplates() {
        processNegativeOrder()

        bucketizeV1TemplatesByOrder()
        populateBucketPriority()
        populateV1Template()

        clusterService.state().metadata.templatesV2().forEach {
            val template = it.value
            val indexPatterns = template.indexPatterns()
            val priority = template.priorityOrZero().toInt()
            val policyIDSetting = simulateTemplate(it.key)
            if (policyIDSetting != null) {
                populateV2ISMTemplateMap(policyIDSetting, indexPatterns, priority)
            }
        }
    }

    // old v1 template can have negative priority
    // map the negative priority to non-negative value
    private fun processNegativeOrder() {
        val negOrderSet = mutableSetOf<Int>()
        clusterService.state().metadata.templates.forEach {
            val policyIDSetting = ManagedIndexSettings.POLICY_ID.get(it.value.settings())
            if (policyIDSetting != "") {
                val priority = it.value.order
                if (priority < 0) {
                    negOrderSet.add(priority)
                }
                // cache pattern and policyID for v1 template
                v1TemplatesWithPolicyID[it.key] = V1TemplateCache(it.value.patterns(), 0, policyIDSetting)
            }
        }
        val sorted = negOrderSet.sorted()
        var p = 0
        for (i in sorted) {
            negOrderToPositive[i] = p++
        }
    }

    private fun normalizePriority(order: Int): Int {
        if (order < 0) return negOrderToPositive[order] ?: 0
        return order + (negOrderToPositive.size)
    }

    private fun bucketizeV1TemplatesByOrder() {
        clusterService.state().metadata.templates.forEach {
            val v1TemplateCache = v1TemplatesWithPolicyID[it.key]
            if (v1TemplateCache != null) {
                val priority = normalizePriority(it.value.order)
                // cache the non-negative priority
                v1TemplatesWithPolicyID[it.key] = v1TemplateCache.copy(order = priority)

                val bucket = v1orderToTemplatesName[priority]
                if (bucket == null) {
                    v1orderToTemplatesName[priority] = mutableListOf(it.key)
                } else {
                    // add later one to start of the list
                    bucket.add(0, it.key)
                }
            }
        }
    }

    private fun populateBucketPriority() {
        v1orderToTemplatesName.forEach { (order, templateNames) ->
            var increase = 0
            templateNames.forEach {
                val v1TemplateCache = v1TemplatesWithPolicyID[it]
                if (v1TemplateCache != null) {
                    val cachePriority = v1TemplateCache.order
                    v1TemplatesWithPolicyID[it] = v1TemplateCache
                        .copy(order = cachePriority + increase)
                }
                increase++
            }
            v1orderToBucketIncrement[order] = templateNames.size - 1
        }
    }

    private fun populateV1Template() {
        val allOrders = v1orderToTemplatesName.keys.toList().sorted()
        allOrders.forEachIndexed { ind, order ->
            val smallerOrders = allOrders.subList(0, ind)
            val increments = smallerOrders.mapNotNull { v1orderToBucketIncrement[it] }.sum()

            val templates = v1orderToTemplatesName[order]
            templates?.forEach {
                val v1TemplateCache = v1TemplatesWithPolicyID[it]
                if (v1TemplateCache != null) {
                    val policyID = v1TemplateCache.policyID
                    val indexPatterns = v1TemplateCache.patterns
                    val priority = v1TemplateCache.order + increments
                    saveISMTemplateToMap(policyID, ISMTemplate(indexPatterns, priority, lastUpdatedTime))
                }
            }
        }
    }

    private fun saveISMTemplateToMap(policyID: String, ismTemplate: ISMTemplate) {
        val policyToISMTemplate = ismTemplateMap[policyID]
        if (policyToISMTemplate != null) {
            policyToISMTemplate.add(ismTemplate)
        } else {
            ismTemplateMap[policyID] = mutableListOf(ismTemplate)
        }
    }

    private suspend fun simulateTemplate(templateName: String): String? {
        val request = SimulateTemplateAction.Request(templateName)
        val response: SimulateIndexTemplateResponse =
            client.suspendUntil { execute(SimulateTemplateAction.INSTANCE, request, it) }

        var policyIDSetting: String? = null
        withContext(Dispatchers.IO) {
            val out = BytesStreamOutput().also { response.writeTo(it) }
            val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
            val resolvedTemplate = sin.readOptionalWriteable(::Template)
            if (resolvedTemplate != null) {
                policyIDSetting = ManagedIndexSettings.POLICY_ID.get(resolvedTemplate.settings())
            }
        }
        return policyIDSetting
    }

    private fun populateV2ISMTemplateMap(policyID: String, indexPatterns: List<String>, priority: Int) {
        var v1Increment = 0
        val v1MaxOrder = v1orderToBucketIncrement.keys.maxOrNull()
        if (v1MaxOrder != null) {
            v1Increment = v1MaxOrder + v1orderToBucketIncrement.values.sum()
        }

        saveISMTemplateToMap(policyID, ISMTemplate(indexPatterns, normalizePriority(priority) + v1Increment, lastUpdatedTime))
    }

    private suspend fun getISMPolicies() {
        if (ismTemplateMap.isEmpty()) return

        val mReq = MultiGetRequest()
        ismTemplateMap.keys.forEach { mReq.add(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, it) }
        try {
            val mRes: MultiGetResponse = client.suspendUntil { multiGet(mReq, it) }
            policiesToUpdate.clear()
            mRes.forEach {
                if (it.response != null && !it.response.isSourceEmpty && !it.isFailed) {
                    val response = it.response
                    var policy: Policy? = null
                    try {
                        policy = XContentHelper.createParser(
                            xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE,
                            response.sourceAsBytesRef,
                            XContentType.JSON
                        ).use { xcp ->
                            xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, Policy.Companion::parse)
                        }
                    } catch (e: Exception) {
                        logger.error("Failed to parse policy [${response.id}] when migrating templates", e)
                    }

                    if (policy?.ismTemplate == null) {
                        policiesToUpdate[it.id] = Pair(response.seqNo, response.primaryTerm)
                    }
                }
            }
        } catch (e: ActionRequestValidationException) {
            logger.warn("ISM config index not exists when migrating templates.")
        }
    }

    private suspend fun updateISMPolicies() {
        if (policiesToUpdate.isEmpty()) return

        if (!imIndices.attemptUpdateConfigIndexMapping()) {
            logger.error("Failed to update config index mapping.")
            return
        }

        var requests = mutableListOf<UpdateRequest>()
        policiesToUpdate.forEach { policyID, (seqNo, priTerm) ->
            val ismTemplates = ismTemplateMap[policyID]
            if (ismTemplates != null)
                requests.add(updateISMTemplateRequest(policyID, ismTemplates, seqNo, priTerm))
        }

        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val failedRequests = mutableListOf<UpdateRequest>()
            var retryCause: Exception? = null
            requests.forEach { req ->
                var res: UpdateResponse? = null
                try {
                    res = client.suspendUntil { update(req, it) }
                    logger.info("update policy for ${req.id()}")
                    if (res?.result == DocWriteResponse.Result.UPDATED) {
                        policiesToUpdate.remove(req.id())
                    }
                } catch (e: Exception) {
                    logger.info("failed to update policy for ${req.id()}")
                    if (res?.status() == RestStatus.TOO_MANY_REQUESTS) {
                        failedRequests.add(req)
                        retryCause = e
                    } else {
                        logger.error("Failed to update policy ${req.id()} with ISM template", e)
                    }
                }
            }

            if (failedRequests.isNotEmpty()) {
                requests = failedRequests
                throw ExceptionsHelper.convertToOpenSearchException(retryCause)
            }
        }
    }

    private fun cleanCache() {
        ismTemplateMap.clear()
        v1TemplatesWithPolicyID.clear()
        v1orderToTemplatesName.clear()
        v1orderToBucketIncrement.clear()
        negOrderToPositive.clear()
        policiesToUpdate.clear()
    }
}

data class V1TemplateCache(
    val patterns: List<String>,
    val order: Int,
    val policyID: String
)

typealias policyID = String
typealias templateName = String
typealias seqNoPrimaryTerm = Pair<Long, Long>
