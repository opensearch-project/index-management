/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.action.ActionListener
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
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getUuidsForClosedIndices
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexConfigIndexRequest
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.validateUserConfiguration
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.Exception
import java.time.Duration
import java.time.Instant

private val log = LogManager.getLogger(TransportAddPolicyAction::class.java)

@Suppress("SpreadOperator")
class TransportAddPolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    val indexNameExpressionResolver: IndexNameExpressionResolver
) : HandledTransportAction<AddPolicyRequest, ISMStatusResponse>(
    AddPolicyAction.NAME, transportService, actionFilters, ::AddPolicyRequest
) {

    @Volatile private var jobInterval = ManagedIndexSettings.JOB_INTERVAL.get(settings)
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.JOB_INTERVAL) {
            jobInterval = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: AddPolicyRequest, listener: ActionListener<ISMStatusResponse>) {
        AddPolicyHandler(client, listener, request).start()
    }

    inner class AddPolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: AddPolicyRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {
        private lateinit var startTime: Instant
        private lateinit var policy: Policy
        private val resolvedIndices = mutableListOf<String>()
        private val indicesToAdd = mutableMapOf<String, String>() // uuid: name
        private val failedIndices: MutableList<FailedIndex> = mutableListOf()

        fun start() {
            if (!validateUserConfiguration(user, filterByEnabled, actionListener)) {
                return
            }
            val requestedIndices = mutableListOf<String>()
            request.indices.forEach { index ->
                requestedIndices.addAll(
                    indexNameExpressionResolver.concreteIndexNames(
                        clusterService.state(),
                        IndicesOptions.lenientExpand(),
                        true,
                        index
                    )
                )
            }
            if (requestedIndices.isEmpty()) {
                // Nothing to do will ignore since found no matching indices
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                return
            }
            if (user == null) {
                resolvedIndices.addAll(requestedIndices)
                getPolicy()
            } else {
                validateAndGetPolicy(0, requestedIndices)
            }
        }

        /**
         * We filter the requested indices to the indices user has permission to manage and apply policies only on top of those
         */
        private fun validateAndGetPolicy(current: Int, indices: List<String>) {
            val request = ManagedIndexRequest().indices(indices[current])
            client.execute(
                ManagedIndexAction.INSTANCE,
                request,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                        resolvedIndices.add(indices[current])
                        proceed(current, indices)
                    }

                    override fun onFailure(e: Exception) {
                        when (e is OpenSearchSecurityException) {
                            true -> {
                                proceed(current, indices)
                            }
                            false -> {
                                // failing the request for any other exception
                                actionListener.onFailure(e)
                            }
                        }
                    }
                }
            )
        }

        private fun proceed(current: Int, indices: List<String>) {
            if (current < indices.count() - 1) {
                validateAndGetPolicy(current + 1, indices)
            } else {
                // sanity check that there are indices - if none then return
                if (resolvedIndices.isEmpty()) {
                    actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                    return
                }
                getPolicy()
            }
        }

        private fun getPolicy() {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.policyID)

            client.threadPool().threadContext.stashContext().use {
                if (!validateUserConfiguration(user, filterByEnabled, actionListener)) {
                    return
                }
                client.get(getRequest, ActionListener.wrap(::onGetPolicyResponse, ::onFailure))
            }
        }

        private fun onGetPolicyResponse(response: GetResponse) {
            if (!response.isExists || response.isSourceEmpty) {
                actionListener.onFailure(OpenSearchStatusException("Could not find policy=${request.policyID}", RestStatus.NOT_FOUND))
                return
            }
            this.policy = parseFromGetResponse(response, xContentRegistry, Policy.Companion::parse)
            if (!userHasPermissionForResource(user, policy.user, filterByEnabled, "policy", request.policyID, actionListener)) {
                return
            }

            IndexUtils.checkAndUpdateConfigIndexMapping(
                clusterService.state(),
                client.admin().indices(),
                ActionListener.wrap(::onUpdateMapping, ::onFailure)
            )
        }

        private fun onUpdateMapping(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
                getClusterState()
            } else {
                log.error("Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping.")

                actionListener.onFailure(
                    OpenSearchStatusException(
                        "Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping.",
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                )
            }
        }

        @Suppress("SpreadOperator")
        fun getClusterState() {
            val strictExpandOptions = IndicesOptions.strictExpand()

            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(*resolvedIndices.toTypedArray())
                .metadata(true)
                .local(false)
                .waitForTimeout(TimeValue.timeValueMillis(ADD_POLICY_TIMEOUT_IN_MILLIS))
                .indicesOptions(strictExpandOptions)

            startTime = Instant.now()

            client.admin()
                .cluster()
                .state(
                    clusterStateRequest,
                    object : ActionListener<ClusterStateResponse> {
                        override fun onResponse(response: ClusterStateResponse) {
                            response.state.metadata.indices.forEach {
                                indicesToAdd.putIfAbsent(it.value.indexUUID, it.key)
                            }

                            populateLists(response.state)
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                        }
                    }
                )
        }

        private fun populateLists(state: ClusterState) {
            getUuidsForClosedIndices(state).forEach {
                failedIndices.add(FailedIndex(indicesToAdd[it] as String, it, "This index is closed"))
                indicesToAdd.remove(it)
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
                                        "This index already has a policy, use the update policy API to update index policies"
                                    )
                                )
                                indicesToAdd.remove(docId)
                            }
                        }

                        createManagedIndices()
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
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
                    throw OpenSearchTimeoutException("Add policy API timed out after ClusterStateResponse")
                }

                val bulkReq = BulkRequest().timeout(TimeValue.timeValueMillis(bulkReqTimeout))
                indicesToAdd.forEach { (uuid, name) ->
                    bulkReq.add(
                        managedIndexConfigIndexRequest(name, uuid, request.policyID, jobInterval, policy = policy.copy(user = this.user))
                    )
                }

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
                                            "Failed to add policy due to: ${it.failureMessage}"
                                        )
                                    )
                                    indicesToAdd.remove(docId)
                                }
                            }
                            actionListener.onResponse(ISMStatusResponse(indicesToAdd.size, failedIndices))
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
                    }
                )
            } else {
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
            }
        }

        private fun onFailure(t: Exception) {
            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
        }
    }

    companion object {
        const val ADD_POLICY_TIMEOUT_IN_MILLIS = 30000L
    }
}
