/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.index

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.util.RollupFieldValueExpressionResolver
import org.opensearch.indexmanagement.rollup.util.parseRollup
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.validateUserConfiguration
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

// TODO: Field and mappings validations of source and target index, i.e. reject a histogram agg on example_field if its not possible
@Suppress("LongParameterList")
class TransportIndexRollupAction
@Inject
constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val indexManagementIndices: IndexManagementIndices,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val pluginClient: PluginClient,
) : HandledTransportAction<IndexRollupRequest, IndexRollupResponse>(
    IndexRollupAction.NAME, transportService, actionFilters, ::IndexRollupRequest,
) {
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexRollupRequest, listener: ActionListener<IndexRollupResponse>) {
        IndexRollupHandler(client, listener, request).start()
    }

    inner class IndexRollupHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexRollupResponse>,
        private val request: IndexRollupRequest,
        private val user: User? = buildUser(client.threadPool().threadContext, request.rollup.user),
    ) {
        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
                )}",
            )
            if (!validateUserConfiguration(user, filterByEnabled, actionListener)) {
                return
            }
            indexManagementIndices.checkAndUpdateIMConfigIndex(ActionListener.wrap(::onCreateMappingsResponse, actionListener::onFailure))
        }

        private fun onCreateMappingsResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
                if (request.opType() == DocWriteRequest.OpType.CREATE) {
                    if (!validateTargetIndexName()) {
                        return actionListener.onFailure(
                            OpenSearchStatusException(
                                "target_index value is invalid: ${request.rollup.targetIndex}",
                                RestStatus.BAD_REQUEST,
                            ),
                        )
                    }
                    putRollup()
                } else {
                    getRollup()
                }
            } else {
                val message = "Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping."
                log.error(message)
                actionListener.onFailure(OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun getRollup() {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.rollup.id)
            pluginClient.get(getRequest, ActionListener.wrap(::onGetRollup, actionListener::onFailure))
        }

        @Suppress("ReturnCount")
        private fun onGetRollup(response: GetResponse) {
            if (!response.isExists) {
                actionListener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                return
            }

            val rollup: Rollup?
            try {
                rollup = parseRollup(response, xContentRegistry)
            } catch (e: IllegalArgumentException) {
                actionListener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                return
            }
            if (!SecurityUtils.userHasPermissionForResource(user, rollup.user, filterByEnabled, "rollup", rollup.id, actionListener)) {
                return
            }
            val modified = modifiedImmutableProperties(rollup, request.rollup)
            if (modified.isNotEmpty()) {
                return actionListener.onFailure(OpenSearchStatusException("Not allowed to modify $modified", RestStatus.BAD_REQUEST))
            }
            if (!validateTargetIndexName()) {
                return actionListener.onFailure(
                    OpenSearchStatusException(
                        "target_index value is invalid: ${request.rollup.targetIndex}",
                        RestStatus.BAD_REQUEST,
                    ),
                )
            }
            putRollup()
        }

        private fun modifiedImmutableProperties(rollup: Rollup, newRollup: Rollup): List<String> {
            val modified = mutableListOf<String>()
            if (rollup.continuous != newRollup.continuous) modified.add(Rollup.CONTINUOUS_FIELD)
            if (rollup.dimensions != newRollup.dimensions) modified.add(Rollup.DIMENSIONS_FIELD)
            if (rollup.metrics != newRollup.metrics) modified.add(Rollup.METRICS_FIELD)
            if (rollup.sourceIndex != newRollup.sourceIndex) modified.add(Rollup.SOURCE_INDEX_FIELD)
            if (rollup.targetIndex != newRollup.targetIndex) modified.add(Rollup.TARGET_INDEX_FIELD)
            return modified.toList()
        }

        private fun putRollup() {
            val rollup = request.rollup.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion, user = this.user)
            request.index(INDEX_MANAGEMENT_INDEX)
                .id(request.rollup.id)
                .source(rollup.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .timeout(IndexRequest.DEFAULT_TIMEOUT)
            pluginClient.index(
                request,
                object : ActionListener<IndexResponse> {
                    override fun onResponse(response: IndexResponse) {
                        if (response.shardInfo.failed > 0) {
                            val failureReasons = response.shardInfo.failures.joinToString(", ") { it.reason() }
                            actionListener.onFailure(OpenSearchStatusException(failureReasons, response.status()))
                        } else {
                            val status = if (request.opType() == DocWriteRequest.OpType.CREATE) RestStatus.CREATED else RestStatus.OK
                            actionListener.onResponse(
                                IndexRollupResponse(
                                    response.id, response.version, response.seqNo, response.primaryTerm, status,
                                    rollup.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm),
                                ),
                            )
                        }
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(e)
                    }
                },
            )
        }

        private fun validateTargetIndexName(): Boolean {
            val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(request.rollup, request.rollup.targetIndex)
            return !targetIndexResolvedName.contains("*") && !targetIndexResolvedName.contains("?")
        }
    }
}
