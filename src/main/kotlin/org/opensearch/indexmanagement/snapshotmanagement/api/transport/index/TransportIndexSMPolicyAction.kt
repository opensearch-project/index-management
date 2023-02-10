/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.index

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.authuser.User
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.contentParser
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.INDEX_SM_POLICY_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.MAXIMUM_POLICIES_PER_REPOSITORY
import org.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.MAXIMUM_SNAPSHOTS_PER_POLICY
import org.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.REPOSITORY_DENY_LIST
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.TransportService
import org.opensearch.indexmanagement.snapshotmanagement.checkRepositoryDenyList
import org.opensearch.indexmanagement.snapshotmanagement.util.SEARCH_MAX_HITS

class TransportIndexSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    private val indexManagementIndices: IndexManagementIndices,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<IndexSMPolicyRequest, IndexSMPolicyResponse>(
    INDEX_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::IndexSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)
    @Volatile private var maxPoliciesPerRepo = MAXIMUM_POLICIES_PER_REPOSITORY.get(settings)
    @Volatile private var maxSnapshotsPerPolicy = MAXIMUM_SNAPSHOTS_PER_POLICY.get(settings)
    @Volatile private var repositoryDenyList = REPOSITORY_DENY_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAXIMUM_POLICIES_PER_REPOSITORY) {
            maxPoliciesPerRepo = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAXIMUM_SNAPSHOTS_PER_POLICY) {
            maxSnapshotsPerPolicy = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REPOSITORY_DENY_LIST) {
            repositoryDenyList = it
        }
    }

    override suspend fun executeRequest(
        request: IndexSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): IndexSMPolicyResponse {
        // If filterBy is enabled and security is disabled or if filter by is enabled and backend role are empty an exception will be thrown
        SecurityUtils.validateUserConfiguration(user, filterByEnabled)

        if (indexManagementIndices.checkAndUpdateIMConfigIndex(log)) {
            log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
        }
        return indexSMPolicy(request, user)
    }

    private suspend fun indexSMPolicy(request: IndexSMPolicyRequest, user: User?): IndexSMPolicyResponse {

        var policy = request.policy.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion, user = user)
        val policyRepository = policy.snapshotConfig["repository"] as String

        checkRepositoryDenyList(repositoryDenyList, policyRepository)
        policy = checkMaxCount(policy)
        checkMaxPoliciesPerRepository(policy, policyRepository)

        val indexReq = request.index(INDEX_MANAGEMENT_INDEX)
            .source(policy.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            .id(policy.id)
            .routing(policy.id) // by default routed by id
        val indexRes: IndexResponse = client.suspendUntil { index(indexReq, it) }

        return IndexSMPolicyResponse(indexRes.id, indexRes.version, indexRes.seqNo, indexRes.primaryTerm, policy, indexRes.status())
    }

    private suspend fun checkMaxPoliciesPerRepository(policy: SMPolicy, policyRepository: String) {
        val policies: List<SMPolicy> = try {
            val searchRequest = SearchRequest()
                .source(
                    SearchSourceBuilder().query(
                        QueryBuilders.existsQuery(SMPolicy.SM_TYPE)
                    ).size(SEARCH_MAX_HITS)
                )
                .indices(INDEX_MANAGEMENT_INDEX)
            val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            response.hits.hits.map {
                contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, SMPolicy.Companion::parse)
            }
        } catch (e: IndexNotFoundException) {
            emptyList()
        } catch (e: Exception) {
            throw OpenSearchStatusException(
                "Failed to retrieve policies before create/update policy because $e",
                RestStatus.INTERNAL_SERVER_ERROR
            )
        }
        policies.map {
            val policiesWithSameRepo = mutableSetOf(policy.policyName)
            if (it.snapshotConfig["repository"] == policyRepository) {
                policiesWithSameRepo.add(it.policyName)
                if (policiesWithSameRepo.size > maxPoliciesPerRepo) {
                    throw OpenSearchStatusException(
                        "Only $maxPoliciesPerRepo policies are allowed for repository $policyRepository. " +
                            "Please ask admin to update cluster setting ${MAXIMUM_POLICIES_PER_REPOSITORY.key}.",
                        RestStatus.BAD_REQUEST
                    )
                }
            }
        }
    }

    private fun checkMaxCount(policy: SMPolicy): SMPolicy {
        var updatedPolicy = policy
        //  if deletion condition is not null and deletion.maxCount is null or larger than maxSnapshotsPerPolicy, update it
        //  if policy deletion is null and maxSnapshotsPerPolicy is not -1, add policy deletion w/ maximum snapshot condition
        val policyDeletionCondition = policy.deletion
        val policyMaxRetentionCount = policy.deletion?.condition?.maxCount
        if (maxSnapshotsPerPolicy != -1) {
            if (policyDeletionCondition != null) {
                if (policyMaxRetentionCount != null && policyMaxRetentionCount > maxSnapshotsPerPolicy ||
                    policyMaxRetentionCount == null
                ) {
                    updatedPolicy = policy.copy(
                        deletion = policyDeletionCondition.copy(
                            condition = policyDeletionCondition.condition.copy(
                                maxCount = maxSnapshotsPerPolicy
                            )
                        )
                    )
                    log.warn(
                        "Update the deletion.condition.max_count of policy ${policy.policyName} to " +
                            "the value of cluster setting ${MAXIMUM_SNAPSHOTS_PER_POLICY.key}."
                    )
                }
            }
            if (policyDeletionCondition == null) {
                updatedPolicy = policy.copy(
                    deletion = SMPolicy.Deletion(
                        schedule = policy.creation.schedule,
                        condition = SMPolicy.DeleteCondition(
                            minCount = 1,
                            maxCount = maxSnapshotsPerPolicy
                        )
                    )
                )
                log.warn(
                    "Create deletion condition with max_count of cluster setting ${MAXIMUM_SNAPSHOTS_PER_POLICY.key}'s value " +
                        "for policy ${policy.policyName}."
                )
            }
        }
        return updatedPolicy
    }
}
