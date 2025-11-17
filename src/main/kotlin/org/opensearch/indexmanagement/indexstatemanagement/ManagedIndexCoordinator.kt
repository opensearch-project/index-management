/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

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
import org.opensearch.action.search.ClearScrollAction
import org.opensearch.action.search.ClearScrollRequest
import org.opensearch.action.search.ClearScrollResponse
import org.opensearch.action.search.SearchPhaseExecutionException
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.SearchScrollRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.update.UpdateRequest
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.routing.Preference
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.lifecycle.LifecycleListener
import org.opensearch.common.regex.Regex
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.authuser.User
import org.opensearch.core.index.Index
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.coordinator.ClusterStateManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.mgetManagedIndexMetadata
import org.opensearch.indexmanagement.indexstatemanagement.settings.LegacyOpenDistroManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.AUTO_MANAGE
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.COORDINATOR_BACKOFF_COUNT
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.COORDINATOR_BACKOFF_MILLIS
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.INDEX_STATE_MANAGEMENT_ENABLED
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.JITTER
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.JOB_INTERVAL
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.SWEEP_PERIOD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.ISM_TEMPLATE_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexMetadataRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexRequest
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
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ISMIndexMetadata
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client

/**
 * Listens for cluster changes to pick up new indices to manage.
 * Sweeps the cluster state and [INDEX_MANAGEMENT_INDEX] for [ManagedIndexConfig].
 *
 * This class listens for [ClusterChangedEvent] to pick up on [ManagedIndexConfig] to create or delete.
 * Also sets up a background process that sweeps the cluster state for [ClusterStateManagedIndexConfig]
 * and the [INDEX_MANAGEMENT_INDEX] for current managed index jobs. It will then compare these
 * ManagedIndices to appropriately create or delete each [ManagedIndexConfig]. Each node that has
 * the [IndexManagementPlugin] installed will have an instance of this class, but only the elected
 * cluster manager node will set up the background sweep process and listen for [ClusterChangedEvent].
 *
 * We do not allow updating to a new policy through Coordinator as this can have bad side effects. If
 * a user wants to update an existing [ManagedIndexConfig] to a new policy (or updated version of policy)
 * then they must use the ChangePolicy API.
 */
@Suppress("TooManyFunctions", "ReturnCount", "NestedBlockDepth", "LongParameterList")
@OpenForTesting
class ManagedIndexCoordinator(
    private val settings: Settings,
    private val client: PluginClient,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    indexManagementIndices: IndexManagementIndices,
    private val indexMetadataProvider: IndexMetadataProvider,
    private val xContentRegistry: NamedXContentRegistry,
) : LifecycleListener(),
    ClusterStateListener,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("ManagedIndexCoordinator")) {
    private val logger = LogManager.getLogger(javaClass)
    private val ismIndices = indexManagementIndices

    private var scheduledFullSweep: Scheduler.Cancellable? = null

    @Volatile private var lastFullSweepTimeNano = System.nanoTime()

    @Volatile private var indexStateManagementEnabled = INDEX_STATE_MANAGEMENT_ENABLED.get(settings)

    @Volatile private var sweepPeriod = SWEEP_PERIOD.get(settings)

    @Volatile private var retryPolicy =
        BackoffPolicy.constantBackoff(COORDINATOR_BACKOFF_MILLIS.get(settings), COORDINATOR_BACKOFF_COUNT.get(settings))

    @Volatile private var jobInterval = JOB_INTERVAL.get(settings)

    @Volatile private var jobJitter = JITTER.get(settings)

    @Volatile private var isClusterManager = false

    @Volatile private var onClusterManagerTimeStamp: Long = 0L

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
        clusterService.clusterSettings.addSettingsUpdateConsumer(COORDINATOR_BACKOFF_MILLIS, COORDINATOR_BACKOFF_COUNT) { millis, count ->
            retryPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    private fun executorName(): String = ThreadPool.Names.MANAGEMENT

    fun onClusterManager() {
        onClusterManagerTimeStamp = System.currentTimeMillis()
        logger.info("Cache cluster manager node onClusterManager time: $onClusterManagerTimeStamp")

        // Init background sweep when promoted to being cluster manager
        initBackgroundSweep()
    }

    fun offClusterManager() {
        // Cancel background sweep when demoted from being cluster manager
        scheduledFullSweep?.cancel()
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        // Instead of using a LocalNodeMasterListener to track cluster manager changes, this service will
        // track them here to avoid conditions where cluster manager listener events run after other
        // listeners that depend on what happened in the cluster manager listener
        if (this.isClusterManager != event.localNodeClusterManager()) {
            this.isClusterManager = event.localNodeClusterManager()
            if (this.isClusterManager) {
                onClusterManager()
            } else {
                offClusterManager()
            }
        }

        if (!isIndexStateManagementEnabled()) return

        if (event.isNewCluster) return

        if (!event.localNodeClusterManager()) return

        if (!event.metadataChanged()) return

        launch { sweepClusterChangedEvent(event) }
    }

    override fun afterStart() {
        initBackgroundSweep()
    }

    override fun beforeStop() {
        scheduledFullSweep?.cancel()
    }

    private fun enable() {
        initBackgroundSweep()
        indexStateManagementEnabled = true

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
    }

    private suspend fun reenableJobs() {
        /*
         * Iterate through all indices and create update requests to update the ManagedIndexConfig for indices that
         * meet the following conditions:
         *   1. Is being managed (has managed-index)
         *   2. Does not have a completed Policy
         *   3. Does not have a failed Policy
         */

        // If IM config doesn't exist skip
        if (!ismIndices.indexManagementIndexExists()) return
        val currentManagedIndexUuids = sweepManagedIndexJobs(client)
        val metadataList = client.mgetManagedIndexMetadata(currentManagedIndexUuids)
        val managedIndicesToEnableReq = mutableListOf<UpdateRequest>()
        metadataList.forEach {
            val metadata = it?.first
            if (metadata != null && !(metadata.isPolicyCompleted || metadata.isFailed)) {
                managedIndicesToEnableReq.add(updateEnableManagedIndexRequest(metadata.indexUuid))
            }
        }

        updateManagedIndices(managedIndicesToEnableReq)
    }

    private fun isIndexStateManagementEnabled(): Boolean = indexStateManagementEnabled == true

    /**
     * create or clean job document and metadata
     */
    @OpenForTesting
    suspend fun sweepClusterChangedEvent(event: ClusterChangedEvent) {
        // If IM config doesn't exist skip
        if (!ismIndices.indexManagementIndexExists()) {
            logger.debug("[.opendistro-ism-config] config index does not exist")
            return
        }
        // indices delete event
        var removeManagedIndexReq = emptyList<DocWriteRequest<*>>()
        var indicesToClean = emptyList<Index>()
        if (event.indicesDeleted().isNotEmpty()) {
            val managedIndices = getManagedIndices(event.indicesDeleted().map { it.uuid })
            val deletedIndices = event.indicesDeleted().map { it.name }
            val allIndicesUuid =
                indexMetadataProvider.getMultiTypeISMIndexMetadata(indexNames = deletedIndices).map { (_, metadataMapForType) ->
                    metadataMapForType.values.map { it.indexUuid }
                }.flatten().toSet()
            // Check if the deleted index uuid is still part of any metadata service in the cluster and has an existing managed index job
            indicesToClean = event.indicesDeleted().filter { it.uuid in managedIndices.keys && !allIndicesUuid.contains(it.uuid) }
            removeManagedIndexReq = indicesToClean.map { deleteManagedIndexRequest(it.uuid) }
        }

        val updateMatchingIndexReq = createManagedIndexRequests(event.state(), event.indicesCreated())

        updateManagedIndices(updateMatchingIndexReq + removeManagedIndexReq)

        clearManagedIndexMetaData(indicesToClean.map { deleteManagedIndexMetadataRequest(it.uuid) })
    }

    /**
     * build requests to create jobs for indices matching ISM templates
     */
    @Suppress("NestedBlockDepth", "ComplexCondition")
    private suspend fun createManagedIndexRequests(
        clusterState: ClusterState,
        indexNames: List<String>,
    ): List<DocWriteRequest<*>> {
        val updateManagedIndexReqs = mutableListOf<DocWriteRequest<IndexRequest>>()
        if (indexNames.isEmpty()) return updateManagedIndexReqs

        val policiesWithTemplates = getPoliciesWithISMTemplates()
        // We use the metadata provider to give us the correct uuid for the index when creating managed index for the index
        val ismIndicesMetadata: Map<String, ISMIndexMetadata> = indexMetadataProvider.getISMIndexMetadataByType(indexNames = indexNames)
        // Iterate over each unmanaged hot/warm index and if it matches an ISM template add a managed index config index request
        indexNames.forEach { indexName ->
            val defaultIndexMetadataService = indexMetadataProvider.services[DEFAULT_INDEX_TYPE] as DefaultIndexMetadataService
            // If there is a custom index uuid associated with the index, we do not auto manage it
            // This is because cold index uses custom uuid, and we do not auto manage cold-to-warm index
            val indexMetadata = clusterState.metadata.index(indexName)
            val wasOffCluster = defaultIndexMetadataService.getIndexUUID(indexMetadata) != indexMetadata.indexUUID
            val ismIndexMetadata = ismIndicesMetadata[indexName]
            // We try to find lookup name instead of using index name as datastream indices need the alias to match policy
            val lookupName = findIndexLookupName(indexName, clusterState)
            if (lookupName != null && !indexMetadataProvider.isUnManageableIndex(lookupName) && ismIndexMetadata != null && !wasOffCluster) {
                val creationDate = ismIndexMetadata.indexCreationDate
                val indexUuid = ismIndexMetadata.indexUuid
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
                                jobJitter,
                            ),
                        )
                    }
            }
        }

        return updateManagedIndexReqs
    }

    private fun findIndexLookupName(indexName: String, clusterState: ClusterState): String? {
        if (clusterState.metadata.hasIndex(indexName)) {
            val indexMetadata = clusterState.metadata.index(indexName)
            val autoManage =
                if (indexMetadata.settings.get(AUTO_MANAGE.key).isNullOrBlank()) {
                    LegacyOpenDistroManagedIndexSettings.AUTO_MANAGE.get(indexMetadata.settings)
                } else {
                    AUTO_MANAGE.get(indexMetadata.settings)
                }
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
     * Find a policy that has the highest priority ism template with matching index pattern to the index and is created before index creation date. If
     * the policy has user, ensure that the user can manage the index if not find the one that can.
     * */
    private suspend fun findMatchingPolicy(indexName: String, creationDate: Long, policies: List<Policy>): Policy? {
        val priorityPolicyMap = mutableMapOf<Int, Policy>()
        policies.forEach { policy ->
            var highestPriorityForPolicy = -1
            policy.ismTemplate?.filter { template ->
                template.lastUpdatedTime.toEpochMilli() < creationDate
            }?.forEach { template ->
                if (matchesIndexPatterns(indexName, template.indexPatterns)) {
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

    /**
     * Checks if an index name matches the given index patterns, supporting exclusion patterns prefixed with `-`.
     * The index must match at least one inclusion pattern and must not match any exclusion patterns.
     *
     * @param indexName The name of the index to check
     * @param patterns List of index patterns, where patterns starting with `-` are exclusion patterns
     * @return true if the index matches (included and not excluded), false otherwise
     */
    private fun matchesIndexPatterns(indexName: String, patterns: List<String>): Boolean {
        val inclusionPatterns = mutableListOf<String>()
        val exclusionPatterns = mutableListOf<String>()

        // Separate inclusion and exclusion patterns
        patterns.forEach { pattern ->
            if (pattern.startsWith("-")) {
                exclusionPatterns.add(pattern.substring(1))
            } else {
                inclusionPatterns.add(pattern)
            }
        }

        // Check if index matches any inclusion pattern
        // Note: inclusionPatterns.isEmpty() is prevented by validation in ISMTemplateService
        val matchesInclusion = inclusionPatterns.any { pattern ->
            Regex.simpleMatch(pattern, indexName)
        }

        if (!matchesInclusion) {
            return false
        }

        // Check if index matches any exclusion pattern
        val matchesExclusion = exclusionPatterns.any { pattern ->
            Regex.simpleMatch(pattern, indexName)
        }

        // Return true only if matches inclusion and does not match exclusion
        return !matchesExclusion
    }

    private suspend fun canPolicyManagedIndex(policy: Policy, indexName: String): Boolean {
        if (policy.user != null) {
            try {
                val request = ManagedIndexRequest().indices(indexName)
                // TODO Do we need to continue to support injected user?
                withClosableContext(IndexManagementSecurityContext("ApplyPolicyOnIndexCreation", settings, threadPool.threadContext, policy.user)) {
                    client.suspendUntil<Client, AcknowledgedResponse> { execute(ManagedIndexAction.INSTANCE, request, it) }
                }
            } catch (e: OpenSearchSecurityException) {
                logger.debug("Skipping applying policy ${policy.id} on $indexName as the policy user is missing permissions", e)
                return false
            } catch (e: Exception) {
                // Ignore other exceptions
            }
        }

        return true
    }

    private suspend fun getPoliciesWithISMTemplates(): List<Policy> {
        val errorMessage = "Failed to get ISM policies with templates"
        val searchRequest =
            SearchRequest()
                .source(
                    SearchSourceBuilder().query(
                        QueryBuilders.existsQuery(ISM_TEMPLATE_FIELD),
                    ).size(MAX_HITS).seqNoAndPrimaryTerm(true),
                )
                .indices(INDEX_MANAGEMENT_INDEX)
                .preference(Preference.PRIMARY_FIRST.type())

        return try {
            val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            parseFromSearchResponse(response, xContentRegistry, Policy.Companion::parse)
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
     * This background sweep will only be initialized if the local node is the elected cluster manager node.
     * Creates a runnable that is executed as a coroutine in the shared pool of threads on JVM.
     */
    @OpenForTesting
    fun initBackgroundSweep() {
        // If ISM is disabled return early
        if (!isIndexStateManagementEnabled()) return

        // Do not set up background sweep if we're not the elected cluster manager node
        if (!clusterService.state().nodes().isLocalNodeElectedClusterManager) return

        // Cancel existing background sweep
        scheduledFullSweep?.cancel()

        // Set up an anti-entropy/self-healing background sweep, in case we fail to create a ManagedIndexConfig job
        val scheduledSweep =
            Runnable {
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
        // If IM config doesn't exist skip
        if (!ismIndices.indexManagementIndexExists()) return

        // Get all current managed indices uuids
        val currentManagedIndexUuids = sweepManagedIndexJobs(client)

        // get all indices in the cluster state
        val currentIndices = indexMetadataProvider.getISMIndexMetadataByType(indexNames = listOf("*"))
        val unManagedIndices = getUnManagedIndices(currentIndices, currentManagedIndexUuids.toHashSet())

        // Get the matching policyIds for applicable indices
        val updateMatchingIndicesReqs =
            createManagedIndexRequests(
                clusterService.state(), unManagedIndices.map { (indexName, _) -> indexName },
            )

        // check all managed indices, if the index has already been deleted
        val allIndicesUuids = indexMetadataProvider.getAllISMIndexMetadata().map { it.indexUuid }
        val managedIndicesToDelete = getManagedIndicesToDelete(allIndicesUuids, currentManagedIndexUuids)
        val deleteManagedIndexRequests = managedIndicesToDelete.map { deleteManagedIndexRequest(it) }

        updateManagedIndices(updateMatchingIndicesReqs + deleteManagedIndexRequests)

        // clean metadata of un-managed index
        val indicesToDeleteMetadataFrom = unManagedIndices.map { (_, ismMetadata) -> ismMetadata.indexUuid } + managedIndicesToDelete
        clearManagedIndexMetaData(indicesToDeleteMetadataFrom.map { deleteManagedIndexMetadataRequest(it) })

        lastFullSweepTimeNano = System.nanoTime()
    }

    private fun getUnManagedIndices(allIndices: Map<String, ISMIndexMetadata>, managedIndexUuids: Set<String>): Map<String, ISMIndexMetadata> {
        val unManagedIndices = mutableMapOf<String, ISMIndexMetadata>()
        allIndices.forEach { (indexName, ismMetadata) ->
            if (ismMetadata.indexUuid !in managedIndexUuids) {
                unManagedIndices[indexName] = ismMetadata
            }
        }
        return unManagedIndices
    }

    /**
     * Sweeps the [INDEX_MANAGEMENT_INDEX] for ManagedIndices.
     *
     * Sweeps the [INDEX_MANAGEMENT_INDEX] for ManagedIndices and only fetches the index_uuid
     *
     * @return list of IndexUuid.
     */
    @OpenForTesting
    suspend fun sweepManagedIndexJobs(client: Client): List<String> {
        val managedIndexUuids = mutableListOf<String>()

        // if # of documents below 10k, don't use scroll search
        val countReq = getSweptManagedIndexSearchRequest(size = 0)
        val countRes: SearchResponse = client.suspendUntil { search(countReq, it) }
        val totalHits = countRes.hits.totalHits ?: return managedIndexUuids

        if (totalHits.value >= MAX_HITS) {
            val scrollIDsToClear = mutableSetOf<String>()
            try {
                val managedIndexSearchRequest = getSweptManagedIndexSearchRequest(scroll = true)
                var response: SearchResponse = client.suspendUntil { search(managedIndexSearchRequest, it) }
                var uuids = transformManagedIndexSearchRes(response)

                while (uuids.isNotEmpty()) {
                    managedIndexUuids.addAll(uuids)
                    val scrollID = response.scrollId
                    scrollIDsToClear.add(scrollID)
                    val scrollRequest = SearchScrollRequest().scrollId(scrollID).scroll(TimeValue.timeValueMinutes(1))
                    response = client.suspendUntil { searchScroll(scrollRequest, it) }
                    uuids = transformManagedIndexSearchRes(response)
                }
            } finally {
                if (scrollIDsToClear.isNotEmpty()) {
                    val clearScrollRequest = ClearScrollRequest()
                    clearScrollRequest.scrollIds(scrollIDsToClear.toList())
                    client.suspendUntil<Client, ClearScrollResponse> { execute(ClearScrollAction.INSTANCE, clearScrollRequest, it) }
                }
            }
            return managedIndexUuids
        }

        val response: SearchResponse = client.suspendUntil { search(getSweptManagedIndexSearchRequest(), it) }
        return transformManagedIndexSearchRes(response)
    }

    fun transformManagedIndexSearchRes(response: SearchResponse): List<String> {
        if (response.isTimedOut || response.failedShards > 0 || response.skippedShards > 0) {
            val errorMsg =
                "Sweep managed indices failed. Timed out: ${response.isTimedOut} | " +
                    "Failed shards: ${response.failedShards} | Skipped shards: ${response.skippedShards}."
            logger.error(errorMsg)
            throw ISMCoordinatorSearchException(message = errorMsg)
        }
        return response.hits.map { it.id }
    }

    /**
     * Get managed-index for indices
     *
     * @return map of IndexUuid to [ManagedIndexConfig]
     */
    private suspend fun getManagedIndices(indexUuids: List<String>): Map<String, ManagedIndexConfig?> {
        if (indexUuids.isEmpty()) return emptyMap()

        val result: MutableMap<String, ManagedIndexConfig?> = mutableMapOf()

        val mReq = MultiGetRequest()
        indexUuids.forEach { mReq.add(INDEX_MANAGEMENT_INDEX, it) }
        val mRes: MultiGetResponse = client.suspendUntil { multiGet(mReq, it) }
        val responses = mRes.responses
        if (responses.first().isFailed) {
            // config index may not initialise yet
            logger.error("get managed-index failed: ${responses.first().failure.failure}")
            return result
        }
        mRes.forEach {
            if (it.response.isExists) {
                result[it.id] =
                    contentParser(it.response.sourceAsBytesRef, xContentRegistry).parseWithType(
                        it.response.id, it.response.seqNo, it.response.primaryTerm, ManagedIndexConfig.Companion::parse,
                    )
            }
        }
        return result
    }

    @OpenForTesting
    private suspend fun updateManagedIndices(requests: List<DocWriteRequest<*>>) {
        var requestsToRetry = requests
        if (requestsToRetry.isEmpty()) return

        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkRequest().add(requestsToRetry)
            val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            requestsToRetry =
                failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                    .map { bulkRequest.requests()[it.itemId] }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToOpenSearchException(retryCause)
            }
        }
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
                    if (it.isFailed) {
                        logger.error(
                            "Failed to clear ManagedIndexMetadata for " +
                                "index uuid: [${it.id}], failureMessage: ${it.failureMessage}",
                        )
                    }
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

class ISMCoordinatorSearchException(message: String, cause: Throwable? = null) : Exception(message, cause)
