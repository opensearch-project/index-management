/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.core.index.Index
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.DefaultIndexMetadataService
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider.Companion.EVALUATION_FAILURE_MESSAGE
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getUuidsForClosedIndices
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexMetadataRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexConfigIndexRequest
import org.opensearch.indexmanagement.opensearchapi.IndexManagementSecurityContext
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.opensearchapi.withClosableContext
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ISMIndexMetadata
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.validateUserConfiguration
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.node.NodeClient
import java.time.Duration
import java.time.Instant

private val log = LogManager.getLogger(TransportAddPolicyAction::class.java)

@Suppress("SpreadOperator", "ReturnCount", "LongParameterList")
class TransportAddPolicyAction
@Inject
constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    val indexMetadataProvider: IndexMetadataProvider,
) : HandledTransportAction<AddPolicyRequest, ISMStatusResponse>(
    AddPolicyAction.NAME, transportService, actionFilters, ::AddPolicyRequest,
) {
    @Volatile private var jobInterval = ManagedIndexSettings.JOB_INTERVAL.get(settings)

    @Volatile private var jobJitter = ManagedIndexSettings.JITTER.get(settings)

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.JOB_INTERVAL) {
            jobInterval = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.JITTER) {
            jobJitter = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: AddPolicyRequest, listener: ActionListener<ISMStatusResponse>) {
        AddPolicyHandler(client, listener, request).start()
    }

    @Suppress("TooManyFunctions")
    inner class AddPolicyHandler(
        private val client: Client,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: AddPolicyRequest,
        private val user: User? = buildUser(client.threadPool().threadContext),
    ) {
        private lateinit var startTime: Instant
        private lateinit var policy: Policy
        private val indicesToAdd = mutableMapOf<String, String>() // uuid: name
        private val failedIndices: MutableList<FailedIndex> = mutableListOf()

        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
                )}",
            )
            if (!validateUserConfiguration(user, filterByEnabled, actionListener)) {
                return
            }
            getClusterState()
        }

        @Suppress("SpreadOperator")
        private fun getClusterState() {
            startTime = Instant.now()
            CoroutineScope(Dispatchers.IO).launch {
                val indexNameToMetadata: MutableMap<String, ISMIndexMetadata> = HashMap()
                try {
                    indexNameToMetadata.putAll(indexMetadataProvider.getISMIndexMetadataByType(request.indexType, request.indices))
                } catch (e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    return@launch
                }
                indexNameToMetadata.forEach { (indexName, indexMetadata) ->
                    indicesToAdd.putIfAbsent(indexMetadata.indexUuid, indexName)
                }
                if (indicesToAdd.isEmpty()) {
                    // Nothing to do will ignore since found no matching indices
                    actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                    return@launch
                }
                if (user != null) {
                    withClosableContext(IndexManagementSecurityContext("AddPolicyHandler", settings, client.threadPool().threadContext, user)) {
                        validateIndexPermissions(indicesToAdd.values.toList())
                    }
                }
                removeClosedIndices()
            }
        }

        /**
         * We filter the requested indices to the indices user has permission to manage and apply policies only on top of those
         */
        private suspend fun validateIndexPermissions(indices: List<String>) {
            val permittedIndices = mutableListOf<String>()
            indices.forEach { index ->
                try {
                    client.suspendUntil { execute(ManagedIndexAction.INSTANCE, ManagedIndexRequest().indices(index), it) }
                    permittedIndices.add(index)
                } catch (e: OpenSearchSecurityException) {
                    log.debug("No permissions for index [$index]")
                }
            }

            // sanity check that there are indices - if none then return
            if (permittedIndices.isEmpty()) {
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                return
            }
            // Filter out the indices that the user does not have permissions for
            indicesToAdd.values.removeIf { !permittedIndices.contains(it) }
        }

        private fun removeClosedIndices() {
            // Do another cluster state request to fail closed indices
            if (request.indexType == DEFAULT_INDEX_TYPE) {
                val strictExpandOptions = IndicesOptions.strictExpand()
                val clusterStateRequest =
                    ClusterStateRequest()
                        .clear()
                        .indices(*indicesToAdd.values.toTypedArray())
                        .metadata(true)
                        .local(false)
                        .waitForTimeout(TimeValue.timeValueMillis(ADD_POLICY_TIMEOUT_IN_MILLIS))
                        .indicesOptions(strictExpandOptions)
                client.admin()
                    .cluster()
                    .state(
                        clusterStateRequest,
                        object : ActionListener<ClusterStateResponse> {
                            override fun onResponse(response: ClusterStateResponse) {
                                val defaultIndexMetadataService = indexMetadataProvider.services[DEFAULT_INDEX_TYPE] as DefaultIndexMetadataService
                                getUuidsForClosedIndices(response.state, defaultIndexMetadataService).forEach {
                                    failedIndices.add(FailedIndex(indicesToAdd[it] as String, it, "This index is closed"))
                                    indicesToAdd.remove(it)
                                }
                                getPolicy()
                            }

                            override fun onFailure(t: Exception) {
                                actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                            }
                        },
                    )
            } else {
                getPolicy()
            }
        }

        private fun getPolicy() {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.policyID)

            if (!validateUserConfiguration(user, filterByEnabled, actionListener)) {
                return
            }
            client.get(getRequest, ActionListener.wrap(::onGetPolicyResponse, ::onFailure))
        }

        private fun onGetPolicyResponse(response: GetResponse) {
            if (!response.isExists || response.isSourceEmpty) {
                actionListener.onFailure(OpenSearchStatusException("Could not find policy=${request.policyID}", RestStatus.NOT_FOUND))
                return
            }
            try {
                this.policy = parseFromGetResponse(response, xContentRegistry, Policy.Companion::parse)
            } catch (e: IllegalArgumentException) {
                actionListener.onFailure(OpenSearchStatusException("Could not find policy=${request.policyID}", RestStatus.NOT_FOUND))
                return
            }
            if (!userHasPermissionForResource(user, policy.user, filterByEnabled, "policy", request.policyID, actionListener)) {
                return
            }

            IndexUtils.checkAndUpdateConfigIndexMapping(
                clusterService.state(),
                client.admin().indices(),
                ActionListener.wrap(::onUpdateMapping, ::onFailure),
            )
        }

        private fun onUpdateMapping(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
                getExistingManagedIndices()
            } else {
                log.error("Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping.")

                actionListener.onFailure(
                    OpenSearchStatusException(
                        "Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping.",
                        RestStatus.INTERNAL_SERVER_ERROR,
                    ),
                )
            }
        }

        private fun getExistingManagedIndices() {
            // Removing all the unmanageable Indices
            indicesToAdd.entries.removeIf { (uuid, indexName) ->
                val shouldRemove = indexMetadataProvider.isUnManageableIndex(indexName)
                if (shouldRemove) {
                    failedIndices.add(FailedIndex(indexName, uuid, EVALUATION_FAILURE_MESSAGE))
                }
                shouldRemove
            }
            if (indicesToAdd.isEmpty()) {
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                return
            }

            val multiGetReq = MultiGetRequest()
            indicesToAdd.forEach { multiGetReq.add(INDEX_MANAGEMENT_INDEX, it.key) }

            client.multiGet(
                multiGetReq,
                object : ActionListener<MultiGetResponse> {
                    override fun onResponse(response: MultiGetResponse) {
                        response.forEach {
                            if (it.response.isExists) {
                                val docId = it.id // docId is managed index uuid
                                failedIndices.add(
                                    FailedIndex(
                                        indicesToAdd[docId] as String, docId,
                                        "This index already has a policy, use the update policy API to update index policies",
                                    ),
                                )
                                indicesToAdd.remove(docId)
                            }
                        }

                        createManagedIndices()
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                },
            )
        }

        private fun createManagedIndices() {
            if (indicesToAdd.isNotEmpty()) {
                val timeSinceClusterStateRequest: Duration = Duration.between(startTime, Instant.now())

                // Timeout for UpdateSettingsRequest in milliseconds
                val bulkReqTimeout = ADD_POLICY_TIMEOUT_IN_MILLIS - timeSinceClusterStateRequest.toMillis()

                // If after the ClusterStateResponse we go over the timeout for Add Policy (30 seconds), throw an
                // exception since UpdateSettingsRequest cannot have a negative timeout
                if (bulkReqTimeout < 0) {
                    actionListener.onFailure(OpenSearchTimeoutException("Add policy API timed out after ClusterStateResponse"))
                    return
                }

                val bulkReq = BulkRequest().timeout(TimeValue.timeValueMillis(bulkReqTimeout))
                indicesToAdd.forEach { (uuid, name) ->
                    bulkReq.add(
                        managedIndexConfigIndexRequest(name, uuid, request.policyID, jobInterval, policy = policy.copy(user = this.user), jobJitter),
                    )
                }

                bulkReq.refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE

                client.bulk(
                    bulkReq,
                    object : ActionListener<BulkResponse> {
                        override fun onResponse(response: BulkResponse) {
                            response.forEach {
                                val docId = it.id // docId is managed index uuid
                                if (it.isFailed) {
                                    failedIndices.add(
                                        FailedIndex(
                                            indicesToAdd[docId] as String, docId,
                                            "Failed to add policy due to: ${it.failureMessage}",
                                        ),
                                    )
                                    indicesToAdd.remove(docId)
                                }
                            }
                            actionListener.onResponse(ISMStatusResponse(indicesToAdd.size, failedIndices))

                            // best effort to clean up ISM metadata
                            removeMetadatas(indicesToAdd.map { Index(it.value, it.key) })
                        }

                        override fun onFailure(t: Exception) {
                            if (t is ClusterBlockException) {
                                indicesToAdd.forEach { (uuid, name) ->
                                    failedIndices.add(FailedIndex(name, uuid, "Failed to add policy due to ClusterBlockingException: ${t.message}"))
                                }
                                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                            } else {
                                actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                            }
                        }
                    },
                )
            } else {
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
            }
        }

        private fun onFailure(t: Exception) {
            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
        }

        fun removeMetadatas(indices: List<Index>) {
            val request = indices.map { deleteManagedIndexMetadataRequest(it.uuid) }
            val bulkReq = BulkRequest().add(request)
            bulkReq.refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
            client.bulk(
                bulkReq,
                object : ActionListener<BulkResponse> {
                    override fun onResponse(response: BulkResponse) {
                        log.debug("Successfully cleaned metadata for remove policy indices: {}", indices)
                    }

                    override fun onFailure(e: Exception) {
                        log.error("Failed to clean metadata for remove policy indices.", e)
                    }
                },
            )
        }
    }

    companion object {
        const val ADD_POLICY_TIMEOUT_IN_MILLIS = 30000L
    }
}
