/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:Suppress("ReturnCount")
package org.opensearch.indexmanagement.indexstatemanagement

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchPhaseExecutionException
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.update.UpdateRequest
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.regex.Regex
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.authuser.User
import org.opensearch.index.Index
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.coordinator.ClusterStateManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.mgetManagedIndexMetadata
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.AUTO_MANAGE
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.COORDINATOR_BACKOFF_COUNT
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.COORDINATOR_BACKOFF_MILLIS
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.INDEX_STATE_MANAGEMENT_ENABLED
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.JITTER
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.JOB_INTERVAL
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.METADATA_SERVICE_ENABLED
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.SWEEP_PERIOD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.ISM_TEMPLATE_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexMetadataRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.getDeleteManagedIndexRequests
import org.opensearch.indexmanagement.indexstatemanagement.util.getManagedIndicesToDelete
import org.opensearch.indexmanagement.indexstatemanagement.util.getSweptManagedIndexSearchRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.isFailed
import org.opensearch.indexmanagement.indexstatemanagement.util.isPolicyCompleted
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexConfigIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.updateEnableManagedIndexRequest
import org.opensearch.indexmanagement.opensearchapi.IndexManagementSecurityContext
import org.opensearch.indexmanagement.opensearchapi.contentParser
import org.opensearch.indexmanagement.opensearchapi.parseFromSearchResponse
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.opensearchapi.withClosableContext
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool

/**
 * Listens for cluster changes to pick up new indices to manage.
 * Sweeps the cluster state and [INDEX_MANAGEMENT_INDEX] for [ManagedIndexConfig].
 *
 * This class listens for [ClusterChangedEvent] to pick up on [ManagedIndexConfig] to create or delete.
 * Also sets up a background process that sweeps the cluster state for [ClusterStateManagedIndexConfig]
 * and the [INDEX_MANAGEMENT_INDEX] for [SweptManagedIndexConfig]. It will then compare these
 * ManagedIndices to appropriately create or delete each [ManagedIndexConfig]. Each node that has
 * the [IndexManagementPlugin] installed will have an instance of this class, but only the elected
 * master node will set up the background sweep process and listen for [ClusterChangedEvent].
 *
 * We do not allow updating to a new policy through Coordinator as this can have bad side effects. If
 * a user wants to update an existing [ManagedIndexConfig] to a new policy (or updated version of policy)
 * then they must use the ChangePolicy API.
 */
@Suppress("TooManyFunctions")
@OpenForTesting
class ManagedIndexCoordinator(
    private val settings: Settings,
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    indexManagementIndices: IndexManagementIndices,
    private val metadataService: MetadataService
) : ClusterStateListener,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("ManagedIndexCoordinator")),
    LifecycleListener() {

    private val logger = LogManager.getLogger(javaClass)
    private val ismIndices = indexManagementIndices

    private var scheduledFullSweep: Scheduler.Cancellable? = null
    private var scheduledMoveMetadata: Scheduler.Cancellable? = null

    @Volatile private var lastFullSweepTimeNano = System.nanoTime()
    @Volatile private var indexStateManagementEnabled = INDEX_STATE_MANAGEMENT_ENABLED.get(settings)
    @Volatile private var metadataServiceEnabled = METADATA_SERVICE_ENABLED.get(settings)
    @Volatile private var sweepPeriod = SWEEP_PERIOD.get(settings)
    @Volatile private var retryPolicy =
        BackoffPolicy.constantBackoff(COORDINATOR_BACKOFF_MILLIS.get(settings), COORDINATOR_BACKOFF_COUNT.get(settings))
    @Volatile private var jobInterval = JOB_INTERVAL.get(settings)
    @Volatile private var jobJitter = JITTER.get(settings)

    @Volatile private var isMaster = false

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(SWEEP_PERIOD) {
            sweepPeriod = it
            initBackgroundSweep()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(JOB_INTERVAL) {
            jobInterval = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(JITTER) {
            jobJitter = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_STATE_MANAGEMENT_ENABLED) {
            indexStateManagementEnabled = it
            if (!indexStateManagementEnabled) disable() else enable()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(METADATA_SERVICE_ENABLED) {
            metadataServiceEnabled = it
            if (!metadataServiceEnabled) scheduledMoveMetadata?.cancel()
            else initMoveMetadata()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(COORDINATOR_BACKOFF_MILLIS, COORDINATOR_BACKOFF_COUNT) {
            millis, count ->
            retryPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    private fun executorName(): String {
        return ThreadPool.Names.MANAGEMENT
    }

    fun onMaster() {
        // Init background sweep when promoted to being master
        initBackgroundSweep()

        initMoveMetadata()
    }

    fun offMaster() {
        // Cancel background sweep when demoted from being master
        scheduledFullSweep?.cancel()

        scheduledMoveMetadata?.cancel()
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        // Instead of using a LocalNodeMasterListener to track master changes, this service will
        // track them here to avoid conditions where master listener events run after other
        // listeners that depend on what happened in the master listener
        if (this.isMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster()
            if (this.isMaster) {
                onMaster()
            } else {
                offMaster()
            }
        }

        if (!isIndexStateManagementEnabled()) return

        if (event.isNewCluster) return

        if (!event.localNodeMaster()) return

        if (!event.metadataChanged()) return

        launch { sweepClusterChangedEvent(event) }
    }

    override fun afterStart() {
        initBackgroundSweep()

        initMoveMetadata()
    }

    override fun beforeStop() {
        scheduledFullSweep?.cancel()

        scheduledMoveMetadata?.cancel()
    }

    private fun enable() {
        initBackgroundSweep()
        indexStateManagementEnabled = true

        initMoveMetadata()

        // Calling initBackgroundSweep() beforehand runs a sweep ensuring that policies removed from indices
        // and indices being deleted are accounted for prior to re-enabling jobs
        launch {
            try {
                logger.debug("Re-enabling jobs for managed indices")
                reenableJobs()
            } catch (e: Exception) {
                logger.error("Failed to re-enable jobs for managed indices", e)
            }
        }
    }

    private fun disable() {
        scheduledFullSweep?.cancel()
        indexStateManagementEnabled = false

        scheduledMoveMetadata?.cancel()
    }

    private suspend fun reenableJobs() {
        /*
         * Iterate through all indices and create update requests to update the ManagedIndexConfig for indices that
         * meet the following conditions:
         *   1. Is being managed (has managed-index)
         *   2. Does not have a completed Policy
         *   3. Does not have a failed Policy
         */
        val currentManagedIndices = sweepManagedIndexJobs(client, ismIndices.indexManagementIndexExists())
        val metadataList = client.mgetManagedIndexMetadata(currentManagedIndices.map { Index(it.key, it.value.uuid) })
        val managedIndicesToEnableReq = mutableListOf<UpdateRequest>()
        metadataList.forEach {
            val metadata = it?.first
            if (metadata != null && !(metadata.isPolicyCompleted || metadata.isFailed)) {
                managedIndicesToEnableReq.add(updateEnableManagedIndexRequest(metadata.indexUuid))
            }
        }

        updateManagedIndices(managedIndicesToEnableReq, false)
    }

    private fun isIndexStateManagementEnabled(): Boolean = indexStateManagementEnabled == true

    /**
     * create or clean job document and metadata
     */
    @OpenForTesting
    suspend fun sweepClusterChangedEvent(event: ClusterChangedEvent) {
        // indices delete event
        var removeManagedIndexReq = emptyList<DocWriteRequest<*>>()
        var indicesToClean = emptyList<Index>()
        if (event.indicesDeleted().isNotEmpty()) {
            val managedIndices = getManagedIndices(event.indicesDeleted().map { it.uuid })
            indicesToClean = event.indicesDeleted().filter { it.uuid in managedIndices.keys }
            removeManagedIndexReq = indicesToClean.map { deleteManagedIndexRequest(it.uuid) }
        }

        // check if newly created indices match with any ISM templates
        var updateMatchingIndexReq = emptyList<DocWriteRequest<*>>()
        if (event.indicesCreated().isNotEmpty())
            updateMatchingIndexReq = getMatchingIndicesUpdateReq(event.state(), event.indicesCreated())

        updateManagedIndices(updateMatchingIndexReq + removeManagedIndexReq, updateMatchingIndexReq.isNotEmpty())

        clearManagedIndexMetaData(indicesToClean.map { deleteManagedIndexMetadataRequest(it.uuid) })
    }

    /**
     * build requests to create jobs for indices matching ISM templates
     */
    @Suppress("NestedBlockDepth")
    suspend fun getMatchingIndicesUpdateReq(
        clusterState: ClusterState,
        indexNames: List<String>
    ): List<DocWriteRequest<*>> {
        val updateManagedIndexReqs = mutableListOf<DocWriteRequest<IndexRequest>>()
        if (indexNames.isEmpty()) return updateManagedIndexReqs.toList()

        val policiesWithTemplates = getPoliciesWithISMTemplates()

        // Iterate over each unmanaged hot/warm index and if it matches an ISM template add a managed index config index request
        indexNames.forEach { indexName ->
            val lookupName = findIndexLookupName(indexName, clusterState)
            if (lookupName != null) {
                val indexMetadata = clusterState.metadata.index(indexName)
                val creationDate = indexMetadata.creationDate
                val indexUuid = indexMetadata.indexUUID
                findMatchingPolicy(lookupName, creationDate, policiesWithTemplates)
                    ?.let { policy ->
                        logger.info("Index [$indexName] matched ISM policy template and will be managed by ${policy.id}")
                        updateManagedIndexReqs.add(
                            managedIndexConfigIndexRequest(
                                indexName,
                                indexUuid,
                                policy.id,
                                jobInterval,
                                policy,
                                jobJitter
                            )
                        )
                    }
            }
        }

        return updateManagedIndexReqs.toList()
    }

    private fun findIndexLookupName(indexName: String, clusterState: ClusterState): String? {
        if (clusterState.metadata.hasIndex(indexName)) {
            val indexMetadata = clusterState.metadata.index(indexName)
            val autoManage = indexMetadata.settings.getAsBoolean(AUTO_MANAGE.key, true)
            if (autoManage) {
                val isHiddenIndex =
                    IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.settings) || indexName.startsWith(".")
                val indexAbstraction = clusterState.metadata.indicesLookup[indexName]
                val isDataStreamIndex = indexAbstraction?.parentDataStream != null
                if (!isDataStreamIndex && isHiddenIndex) {
                    return null
                }

                return when {
                    isDataStreamIndex -> indexAbstraction?.parentDataStream?.name
                    else -> indexName
                }
            }
        }

        return null
    }

    /**
     * Find a policy that has highest priority ism template with matching index pattern to the index and is created before index creation date. If
     * the policy has user, ensure that the user can manage the index if not find the one that can.
     * */
    private suspend fun findMatchingPolicy(indexName: String, creationDate: Long, policies: List<Policy>): Policy? {
        val patternMatchPredicate = { pattern: String -> Regex.simpleMatch(pattern, indexName) }
        val priorityPolicyMap = mutableMapOf<Int, Policy>()
        policies.forEach { policy ->
            var highestPriorityForPolicy = -1
            policy.ismTemplate?.filter { template ->
                template.lastUpdatedTime.toEpochMilli() < creationDate
            }?.forEach { template ->
                if (template.indexPatterns.stream().anyMatch(patternMatchPredicate)) {
                    if (highestPriorityForPolicy < template.priority) {
                        highestPriorityForPolicy = template.priority
                    }
                }
            }
            if (highestPriorityForPolicy > -1) {
                priorityPolicyMap[highestPriorityForPolicy] = policy
            }
        }

        val previouslyCheckedUsers = mutableSetOf<User>()
        // sorting the applicable policies based on the priority highest to lowest
        val sortedPriorityPolicyMap = priorityPolicyMap.toSortedMap(reverseOrder())
        sortedPriorityPolicyMap.forEach { (_, policy) ->
            if (!previouslyCheckedUsers.contains(policy.user) && canPolicyManagedIndex(policy, indexName)) {
                return policy
            }

            policy.user?.let { previouslyCheckedUsers.add(it) }
        }

        logger.debug("Couldn't find any matching policy with appropriate permissions that can manage index $indexName")
        return null
    }

    suspend fun canPolicyManagedIndex(policy: Policy, indexName: String): Boolean {
        if (policy.user != null) {
            try {
                val request = ManagedIndexRequest().indices(indexName)
                withClosableContext(IndexManagementSecurityContext("ApplyPolicyOnIndexCreation", settings, threadPool.threadContext, policy.user)) {
                    val response: AcknowledgedResponse = client.suspendUntil { execute(ManagedIndexAction.INSTANCE, request, it) }
                }
            } catch (e: OpenSearchSecurityException) {
                logger.debug("Skipping applying policy ${policy.id} on $indexName as the policy user is missing perimissions", e)
                return false
            } catch (e: Exception) {
                // Ignore other exceptions
            }
        }

        return true
    }

    suspend fun getPoliciesWithISMTemplates(): List<Policy> {
        val errorMessage = "Failed to get ISM policies with templates"
        val searchRequest = SearchRequest()
            .source(
                SearchSourceBuilder().query(
                    QueryBuilders.existsQuery(ISM_TEMPLATE_FIELD)
                ).size(MAX_HITS)
            )
            .indices(INDEX_MANAGEMENT_INDEX)

        return try {
            val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            parseFromSearchResponse(response = response, parse = Policy.Companion::parse)
        } catch (ex: IndexNotFoundException) {
            emptyList()
        } catch (ex: ClusterBlockException) {
            logger.error(errorMessage)
            emptyList()
        } catch (e: SearchPhaseExecutionException) {
            logger.error("$errorMessage: $e")
            emptyList()
        } catch (e: Exception) {
            logger.error(errorMessage, e)
            emptyList()
        }
    }

    /**
     * Background sweep process that periodically sweeps for updates to ManagedIndices
     *
     * This background sweep will only be initialized if the local node is the elected master node.
     * Creates a runnable that is executed as a coroutine in the shared pool of threads on JVM.
     */
    @OpenForTesting
    fun initBackgroundSweep() {
        // If ISM is disabled return early
        if (!isIndexStateManagementEnabled()) return

        // Do not setup background sweep if we're not the elected master node
        if (!clusterService.state().nodes().isLocalNodeElectedMaster) return

        // Cancel existing background sweep
        scheduledFullSweep?.cancel()

        // Setup an anti-entropy/self-healing background sweep, in case we fail to create a ManagedIndexConfig job
        val scheduledSweep = Runnable {
            val elapsedTime = getFullSweepElapsedTime()

            // Rate limit to at most one full sweep per sweep period
            // The schedule runs may wake up a few milliseconds early
            // Delta will be giving some buffer on the schedule to allow waking up slightly earlier
            val delta = sweepPeriod.millis - elapsedTime.millis
            if (delta < BUFFER) { // give 20ms buffer.
                launch {
                    try {
                        logger.debug("Performing background sweep of managed indices")
                        sweep()
                    } catch (e: Exception) {
                        logger.error("Failed to sweep managed indices", e)
                    }
                }
            }
        }

        scheduledFullSweep = threadPool.scheduleWithFixedDelay(scheduledSweep, sweepPeriod, executorName())
    }

    fun initMoveMetadata() {
        if (!metadataServiceEnabled) return
        if (!isIndexStateManagementEnabled()) return
        if (!clusterService.state().nodes().isLocalNodeElectedMaster) return
        scheduledMoveMetadata?.cancel()

        if (metadataService.finishFlag) {
            logger.info("Re-enable Metadata Service.")
            metadataService.reenableMetadataService()
        }

        val scheduledJob = Runnable {
            launch {
                try {
                    if (metadataService.finishFlag) {
                        logger.info("Cancel background move metadata process.")
                        scheduledMoveMetadata?.cancel()
                    }

                    logger.info("Performing move cluster state metadata.")
                    metadataService.moveMetadata()
                } catch (e: Exception) {
                    logger.error("Failed to move cluster state metadata", e)
                }
            }
        }

        scheduledMoveMetadata = threadPool.scheduleWithFixedDelay(scheduledJob, TimeValue.timeValueMinutes(1), executorName())
    }

    private fun getFullSweepElapsedTime(): TimeValue =
        TimeValue.timeValueNanos(System.nanoTime() - lastFullSweepTimeNano)

    /**
     * Sweeps the [INDEX_MANAGEMENT_INDEX] and cluster state.
     *
     * Sweeps the [INDEX_MANAGEMENT_INDEX] and cluster state for any [DocWriteRequest] that need to happen
     * and executes them in batch as a bulk request.
     */
    @OpenForTesting
    suspend fun sweep() {
        // get all indices in the cluster state
        val currentIndices = clusterService.state().metadata.indices.values().map { it.value }
            .distinct().filterNotNull()

        val currentManagedIndices = sweepManagedIndexJobs(client, ismIndices.indexManagementIndexExists())

        // check all un-managed indices, if its name matches any template
        val unManagedIndices = currentIndices
            .filter { it.index.uuid !in currentManagedIndices.keys }
            .map { it.index }.distinct()
        val updateMatchingIndicesReqs = getMatchingIndicesUpdateReq(clusterService.state(), unManagedIndices.map { it.name })

        // check all managed indices, if the index has already been deleted
        val deleteManagedIndexRequests =
            getDeleteManagedIndexRequests(currentIndices, currentManagedIndices)

        updateManagedIndices(
            updateMatchingIndicesReqs + deleteManagedIndexRequests,
            updateMatchingIndicesReqs.isNotEmpty()
        )

        // clean metadata of un-managed index
        val indicesToDeleteMetadataFrom =
            unManagedIndices + getManagedIndicesToDelete(currentIndices, currentManagedIndices)
        clearManagedIndexMetaData(indicesToDeleteMetadataFrom.map { deleteManagedIndexMetadataRequest(it.uuid) })

        lastFullSweepTimeNano = System.nanoTime()
    }

    /**
     * Sweeps the [INDEX_MANAGEMENT_INDEX] for ManagedIndices.
     *
     * Sweeps the [INDEX_MANAGEMENT_INDEX] for ManagedIndices and only fetches the index, index_uuid,
     * policy_id, and change_policy fields to convert into a [SweptManagedIndexConfig].
     *
     * @return map of IndexUuid to [SweptManagedIndexConfig].
     */
    @OpenForTesting
    suspend fun sweepManagedIndexJobs(
        client: Client,
        indexManagementIndexExists: Boolean
    ): Map<String, SweptManagedIndexConfig> {
        if (!indexManagementIndexExists) return mapOf()

        val managedIndexSearchRequest = getSweptManagedIndexSearchRequest()
        val response: SearchResponse = client.suspendUntil { search(managedIndexSearchRequest, it) }
        return response.hits.map {
            it.id to contentParser(it.sourceRef).parseWithType(
                NO_ID, it.seqNo,
                it.primaryTerm, SweptManagedIndexConfig.Companion::parse
            )
        }.toMap()
    }

    /**
     * Get managed-index for indices
     *
     * @return map of IndexUuid to [ManagedIndexConfig]
     */
    suspend fun getManagedIndices(indexUuids: List<String>): Map<String, ManagedIndexConfig?> {
        if (indexUuids.isEmpty()) return emptyMap()

        val result: MutableMap<String, ManagedIndexConfig?> = mutableMapOf()

        val mReq = MultiGetRequest()
        indexUuids.forEach { mReq.add(INDEX_MANAGEMENT_INDEX, it) }
        val mRes: MultiGetResponse = client.suspendUntil { multiGet(mReq, it) }
        val responses = mRes.responses
        if (responses.first().isFailed) {
            // config index may not initialised yet
            logger.error("get managed-index failed: ${responses.first().failure.failure}")
            return result
        }
        mRes.forEach {
            if (it.response.isExists) {
                result[it.id] = contentParser(it.response.sourceAsBytesRef).parseWithType(
                    it.response.id, it.response.seqNo, it.response.primaryTerm, ManagedIndexConfig.Companion::parse
                )
            }
        }
        return result
    }

    @OpenForTesting
    suspend fun updateManagedIndices(requests: List<DocWriteRequest<*>>, hasCreateRequests: Boolean = false) {
        var requestsToRetry = requests
        if (requestsToRetry.isEmpty()) return

        if (hasCreateRequests) {
            val created = ismIndices.attemptInitStateManagementIndex(client)
            if (!created) {
                logger.error("Failed to create $INDEX_MANAGEMENT_INDEX")
                return
            }

            val updated = ismIndices.attemptUpdateConfigIndexMapping()
            if (!updated) {
                logger.error("Failed to update mapping for $INDEX_MANAGEMENT_INDEX")
                return
            }
        }

        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkRequest().add(requestsToRetry)
            val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            requestsToRetry = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                .map { bulkRequest.requests()[it.itemId] }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToOpenSearchException(retryCause)
            }
        }
    }

    /**
     * Returns [Index]es not being managed by ISM
     * but still has ISM metadata
     */
    suspend fun getIndicesToRemoveMetadataFrom(unManagedIndices: List<Index>): List<Index> {
        val indicesToRemoveManagedIndexMetaDataFrom = mutableListOf<Index>()
        val metadataList = client.mgetManagedIndexMetadata(unManagedIndices)
        metadataList.forEach {
            val metadata = it?.first
            if (metadata != null)
                indicesToRemoveManagedIndexMetaDataFrom.add(Index(metadata.index, metadata.indexUuid))
        }
        return indicesToRemoveManagedIndexMetaDataFrom
    }

    /**
     * Removes the [ManagedIndexMetaData] from the given list of [Index]es.
     */
    @OpenForTesting
    @Suppress("TooGenericExceptionCaught")
    suspend fun clearManagedIndexMetaData(deleteRequests: List<DocWriteRequest<*>>) {
        if (!ismIndices.indexManagementIndexExists()) return

        try {
            if (deleteRequests.isEmpty()) return

            retryPolicy.retry(logger) {
                val bulkRequest = BulkRequest().add(deleteRequests)
                val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
                bulkResponse.forEach {
                    if (it.isFailed) logger.error(
                        "Failed to clear ManagedIndexMetadata for " +
                            "index uuid: [${it.id}], failureMessage: ${it.failureMessage}"
                    )
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to clear ManagedIndexMetadata ", e)
        }
    }

    companion object {
        const val MAX_HITS = 10_000
        const val BUFFER = 20L
    }
}
