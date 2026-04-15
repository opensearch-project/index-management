/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.IndicesOptions
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.action.TransitionsAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Conditions
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getOldestRolloverTime
import org.opensearch.indexmanagement.indexstatemanagement.util.TransitionConditionContext
import org.opensearch.indexmanagement.indexstatemanagement.util.evaluateConditions
import org.opensearch.indexmanagement.indexstatemanagement.util.hasStatsConditions
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.time.Instant

private val log = LogManager.getLogger(TransportSimulatePolicyAction::class.java)

@Suppress("TooManyFunctions")
class TransportSimulatePolicyAction
@Inject
constructor(
    val client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    val indexMetadataProvider: IndexMetadataProvider,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
) : HandledTransportAction<SimulatePolicyRequest, SimulatePolicyResponse>(
    SimulatePolicyAction.NAME, transportService, actionFilters, ::SimulatePolicyRequest,
) {
    override fun doExecute(task: Task, request: SimulatePolicyRequest, listener: ActionListener<SimulatePolicyResponse>) {
        SimulateHandler(client, listener, request).start()
    }

    inner class SimulateHandler(
        private val client: Client,
        private val actionListener: ActionListener<SimulatePolicyResponse>,
        private val request: SimulatePolicyRequest,
    ) {
        fun start() {
            CoroutineScope(Dispatchers.IO).launch {
                try {
                    val policy = resolvePolicy()
                    val results = mutableListOf<IndexSimulateResult>()
                    for (indexName in expandIndices(request.indices)) {
                        results.add(simulateIndex(indexName, policy))
                    }
                    actionListener.onResponse(SimulatePolicyResponse(results))
                } catch (e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            }
        }

        /**
         * Expands wildcard patterns (containing * or ?) to concrete index names using the cluster
         * state. Patterns that match no indices are silently dropped. Concrete index names (no
         * wildcards) are kept as-is so that [simulateIndex] can return a proper "not found" error
         * for indices that don't exist in the cluster.
         */
        private fun expandIndices(indices: List<String>): List<String> {
            val clusterState = clusterService.state()
            val result = mutableListOf<String>()
            for (index in indices) {
                if (index.contains('*') || index.contains('?')) {
                    result.addAll(
                        indexNameExpressionResolver.concreteIndexNames(
                            clusterState, IndicesOptions.lenientExpand(), index,
                        ),
                    )
                } else {
                    result.add(index)
                }
            }
            return result.distinct()
        }

        /** Returns the policy to simulate: either the inline policy from the request or one fetched by ID. */
        private suspend fun resolvePolicy(): Policy {
            val inlinePolicy = request.policy
            if (inlinePolicy != null) return inlinePolicy

            val policyId = requireNotNull(request.policyId) { "policyId must not be null when policy is not provided" }
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, policyId)
            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            if (!getResponse.isExists) {
                throw OpenSearchStatusException("Policy '$policyId' not found", RestStatus.NOT_FOUND)
            }
            return parseFromGetResponse(getResponse, xContentRegistry, Policy.Companion::parse)
        }

        /** Runs the full simulation for a single index and returns its result. */
        @Suppress("ReturnCount", "CyclomaticComplexMethod")
        private suspend fun simulateIndex(indexName: String, policy: Policy): IndexSimulateResult {
            val clusterState = clusterService.state()
            val indexMetadata = clusterState.metadata().index(indexName)
                ?: return IndexSimulateResult(
                    indexName = indexName, indexUUID = null, policyId = policy.id,
                    isManaged = false, currentState = null, currentAction = null,
                    transitionEvaluation = null, nextState = null,
                    error = "Index '$indexName' not found in cluster",
                )

            val indexUUID = indexMetadata.indexUUID
            val creationDate = Instant.ofEpochMilli(indexMetadata.creationDate)
            val aliasCount = indexMetadata.aliases?.size ?: 0
            val rolloverDate: Instant? = indexMetadata.getOldestRolloverTime()

            val managedMetadata = fetchManagedIndexMetadata(indexUUID)
            val isManaged = managedMetadata != null

            val currentStateName = managedMetadata?.stateMetaData?.name ?: policy.defaultState
            val currentState = policy.states.find { it.name == currentStateName }
                ?: return IndexSimulateResult(
                    indexName = indexName, indexUUID = indexUUID, policyId = policy.id,
                    isManaged = isManaged, currentState = currentStateName, currentAction = null,
                    transitionEvaluation = null, nextState = null,
                    error = "State '$currentStateName' referenced in metadata not found in policy",
                )

            val currentAction: String? = if (managedMetadata != null) {
                currentState.getActionToExecute(managedMetadata, indexMetadataProvider)?.type
            } else {
                currentState.actions.firstOrNull()?.type ?: TransitionsAction.name
            }

            val inTransitionPhase =
                currentAction == TransitionsAction.name ||
                    (managedMetadata != null && currentState.actions.isEmpty())

            if (!inTransitionPhase) {
                return IndexSimulateResult(
                    indexName = indexName, indexUUID = indexUUID, policyId = policy.id,
                    isManaged = isManaged, currentState = currentStateName,
                    currentAction = currentAction, transitionEvaluation = null,
                    nextState = null, error = null,
                )
            }

            val (numDocs, indexSize) = fetchIndexStats(indexName, currentState.transitions)
            val transitionContext = buildTransitionContext(
                creationDate, numDocs, indexSize, managedMetadata, rolloverDate, aliasCount,
            )
            val transitionResults = currentState.transitions.map { evaluateTransition(it, transitionContext) }
            val nextState = transitionResults.firstOrNull { it.conditionMet }?.stateName

            return IndexSimulateResult(
                indexName = indexName, indexUUID = indexUUID, policyId = policy.id,
                isManaged = isManaged, currentState = currentStateName,
                currentAction = TransitionsAction.name, transitionEvaluation = transitionResults,
                nextState = nextState, error = null,
            )
        }

        /** Fetches primary shard doc-count and size stats when any transition requires them. */
        private suspend fun fetchIndexStats(
            indexName: String,
            transitions: List<Transition>,
        ): Pair<Long?, ByteSizeValue?> {
            if (transitions.none { it.hasStatsConditions() }) return Pair(null, null)
            val statsRequest = IndicesStatsRequest().indices(indexName).clear().docs(true)
            val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(statsRequest, it) }
            val docs = statsResponse.primaries.getDocs()
            return Pair(docs?.count ?: 0, ByteSizeValue(docs?.totalSizeInBytes ?: 0))
        }

        /** Builds a [TransitionConditionContext] from index and managed metadata. */
        private fun buildTransitionContext(
            creationDate: Instant,
            numDocs: Long?,
            indexSize: ByteSizeValue?,
            managedMetadata: ManagedIndexMetaData?,
            rolloverDate: Instant?,
            aliasCount: Int,
        ): TransitionConditionContext {
            val stateStartTime = managedMetadata?.stateMetaData?.startTime?.let { Instant.ofEpochMilli(it) }
            val stepStartTime = managedMetadata?.stepMetaData?.startTime?.let { Instant.ofEpochMilli(it) } ?: Instant.now()
            return TransitionConditionContext(
                indexCreationDate = creationDate, numDocs = numDocs, indexSize = indexSize,
                transitionStartTime = stepStartTime, rolloverDate = rolloverDate,
                indexAliasesCount = aliasCount, stateStartTime = stateStartTime,
            )
        }

        /**
         * Fetches the ManagedIndexMetaData for the given index UUID from the ISM config index.
         * Returns null if the index is not currently managed.
         */
        private suspend fun fetchManagedIndexMetadata(indexUUID: String): ManagedIndexMetaData? {
            val mgetReq = MultiGetRequest()
            mgetReq.add(MultiGetRequest.Item(INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(indexUUID)).routing(indexUUID))
            val mgetResponse: MultiGetResponse = client.suspendUntil { multiGet(mgetReq, it) }

            val getResponse = mgetResponse.responses
                .firstOrNull()
                ?.response
                ?.takeIf { it.isExists && it.sourceAsBytesRef != null }
                ?: return null

            return try {
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    getResponse.sourceAsBytesRef, XContentType.JSON,
                )
                ManagedIndexMetaData.parseWithType(xcp, getResponse.id, getResponse.seqNo, getResponse.primaryTerm)
            } catch (e: Exception) {
                log.warn("Failed to parse ManagedIndexMetaData for indexUUID=$indexUUID", e)
                null
            }
        }

        /**
         * Evaluates a single transition against the given context and returns a detailed result
         * that includes the current metric value and the required threshold, not just a boolean.
         */
        @Suppress("CyclomaticComplexMethod")
        private fun evaluateTransition(
            transition: Transition,
            context: TransitionConditionContext,
        ): TransitionSimulateResult {
            val conditions = transition.conditions
                ?: return TransitionSimulateResult(
                    stateName = transition.stateName, conditionMet = true,
                    conditionType = UNCONDITIONAL, currentValue = null, requiredValue = null,
                )

            val now = Instant.now()
            val conditionMet = transition.evaluateConditions(context)

            return when {
                conditions.indexAge != null -> buildResult(
                    transition, conditionMet, Conditions.MIN_INDEX_AGE_FIELD,
                    formatDuration(now.toEpochMilli() - context.indexCreationDate.toEpochMilli()),
                    conditions.indexAge.toString(),
                )

                conditions.docCount != null -> buildResult(
                    transition, conditionMet, Conditions.MIN_DOC_COUNT_FIELD,
                    context.numDocs?.toString() ?: "unavailable",
                    conditions.docCount.toString(),
                )

                conditions.size != null -> buildResult(
                    transition, conditionMet, Conditions.MIN_SIZE_FIELD,
                    context.indexSize?.toString() ?: "unavailable",
                    conditions.size.toString(),
                )

                conditions.cron != null -> buildResult(
                    transition, conditionMet, Conditions.CRON_FIELD,
                    now.toString(),
                    "next fire at ${conditions.cron.getNextExecutionTime(context.transitionStartTime)}",
                )

                conditions.rolloverAge != null -> buildRolloverAgeResult(transition, conditionMet, conditions, context, now)

                conditions.noAlias != null -> {
                    val aliasCount = context.indexAliasesCount ?: 0
                    buildResult(
                        transition, conditionMet, Conditions.NO_ALIAS_FIELD,
                        if (aliasCount == 0) "no aliases" else "$aliasCount alias(es)",
                        if (conditions.noAlias) "no aliases (no_alias=true)" else "at least one alias (no_alias=false)",
                    )
                }

                conditions.minStateAge != null -> buildStateAgeResult(transition, conditionMet, conditions, context, now)

                else -> buildResult(transition, conditionMet, "unknown", null, null)
            }
        }

        private fun buildRolloverAgeResult(
            transition: Transition,
            conditionMet: Boolean,
            conditions: Conditions,
            context: TransitionConditionContext,
            now: Instant,
        ): TransitionSimulateResult {
            val rolloverDate = context.rolloverDate
            return if (rolloverDate == null) {
                buildResult(
                    transition, false, Conditions.MIN_ROLLOVER_AGE_FIELD,
                    "index has never been rolled over", conditions.rolloverAge.toString(),
                )
            } else {
                buildResult(
                    transition, conditionMet, Conditions.MIN_ROLLOVER_AGE_FIELD,
                    formatDuration(now.toEpochMilli() - rolloverDate.toEpochMilli()),
                    conditions.rolloverAge.toString(),
                )
            }
        }

        private fun buildStateAgeResult(
            transition: Transition,
            conditionMet: Boolean,
            conditions: Conditions,
            context: TransitionConditionContext,
            now: Instant,
        ): TransitionSimulateResult {
            val stateStartTime = context.stateStartTime
            return if (stateStartTime == null) {
                buildResult(
                    transition, false, Conditions.MIN_STATE_AGE_FIELD,
                    "state start time unknown (index not yet managed)", conditions.minStateAge.toString(),
                )
            } else {
                buildResult(
                    transition, conditionMet, Conditions.MIN_STATE_AGE_FIELD,
                    formatDuration(now.toEpochMilli() - stateStartTime.toEpochMilli()),
                    conditions.minStateAge.toString(),
                )
            }
        }

        private fun buildResult(
            transition: Transition,
            conditionMet: Boolean,
            conditionType: String,
            currentValue: String?,
            requiredValue: String?,
        ) = TransitionSimulateResult(
            stateName = transition.stateName,
            conditionMet = conditionMet,
            conditionType = conditionType,
            currentValue = currentValue,
            requiredValue = requiredValue,
        )

        /** Formats a duration in milliseconds as a human-readable string (e.g. "3d 4h 20m"). */
        private fun formatDuration(millis: Long): String = TimeValue.timeValueMillis(millis).toString()
    }

    companion object {
        private const val UNCONDITIONAL = "unconditional"
    }
}
