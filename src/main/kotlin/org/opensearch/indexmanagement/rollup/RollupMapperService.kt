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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.indexmanagement.rollup

import java.lang.IllegalArgumentException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.MappingMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Histogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.action.mapping.UpdateRollupMappingAction
import org.opensearch.indexmanagement.rollup.action.mapping.UpdateRollupMappingRequest
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupJobValidationResult
import org.opensearch.indexmanagement.rollup.settings.LegacyOpenDistroRollupSettings
import org.opensearch.indexmanagement.rollup.settings.RollupSettings
import org.opensearch.indexmanagement.rollup.util.isRollupIndex
import org.opensearch.indexmanagement.util.IndexUtils.Companion._META
import org.opensearch.indexmanagement.util.IndexUtils.Companion.getFieldFromMappings
import org.opensearch.indexmanagement.util._DOC
import org.opensearch.transport.RemoteTransportException

// TODO: Validation of fields across source and target indices overwriting existing rollup data
//  and type validation using mappings from source index
// TODO: Wrap client calls in retry for transient failures
@Suppress("TooManyFunctions")
class RollupMapperService(
    val client: Client,
    val clusterService: ClusterService,
    private val indexNameExpressionResolver: IndexNameExpressionResolver
) {

    private val logger = LogManager.getLogger(javaClass)

    // If the index already exists we need to verify it's a rollup index,
    // confirm it does not conflict with existing jobs and is a valid job
    @Suppress("ReturnCount")
    private suspend fun validateAndAttemptToUpdateTargetIndex(rollup: Rollup): RollupJobValidationResult {
        if (!isRollupIndex(rollup.targetIndex, clusterService.state())) {
            return RollupJobValidationResult.Invalid("Target index [${rollup.targetIndex}] is a non rollup index")
        }

        return when (val jobExistsResult = jobExistsInRollupIndex(rollup)) {
            is RollupJobValidationResult.Valid -> jobExistsResult
            is RollupJobValidationResult.Invalid -> updateRollupIndexMappings(rollup)
            is RollupJobValidationResult.Failure -> jobExistsResult
        }
    }

    // This creates the target index if it doesn't already else validate the target index is rollup index
    // If the target index mappings doesn't contain rollup job attempts to update the mappings.
    // TODO: error handling
    @Suppress("ReturnCount")
    suspend fun attemptCreateRollupTargetIndex(job: Rollup, mixedCluster: Boolean): RollupJobValidationResult {
        if (indexExists(job.targetIndex)) {
            return validateAndAttemptToUpdateTargetIndex(job)
        } else {
            val errorMessage = "Failed to create target index [${job.targetIndex}]"
            return try {
                val response = createTargetIndex(job, mixedCluster)
                if (response.isAcknowledged) {
                    updateRollupIndexMappings(job)
                } else {
                    RollupJobValidationResult.Failure(errorMessage)
                }
            } catch (e: RemoteTransportException) {
                val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
                logger.error(errorMessage, unwrappedException)
                RollupJobValidationResult.Failure(errorMessage, unwrappedException)
            } catch (e: Exception) {
                logger.error("$errorMessage because ", e)
                RollupJobValidationResult.Failure(errorMessage, e)
            }
        }
    }

    private suspend fun createTargetIndex(job: Rollup, useLegacySettings: Boolean): CreateIndexResponse {
        val settings = if (useLegacySettings) {
            Settings.builder().put(LegacyOpenDistroRollupSettings.ROLLUP_INDEX.key, true).build()
        } else {
            Settings.builder().put(RollupSettings.ROLLUP_INDEX.key, true).build()
        }
        val request = CreateIndexRequest(job.targetIndex)
            .settings(settings)
            .mapping(_DOC, IndexManagementIndices.rollupTargetMappings, XContentType.JSON)
        // TODO: Perhaps we can do better than this for mappings... as it'll be dynamic for rest
        //  Can we read in the actual mappings from the source index and use that?
        //  Can it have issues with metrics? i.e. an int mapping with 3, 5, 6 added up and divided by 3 for avg is 14/3 = 4.6666
        //  What happens if the first indexing is an integer, i.e. 3 + 3 + 3 = 9/3 = 3 and it saves it as int
        //  and then the next is float and it fails or rounds it up? Does elasticsearch dynamically resolve to int?
        return client.admin().indices().suspendUntil { create(request, it) }
    }

    // Source index can be a pattern so will need to resolve the index to concrete indices and check:
    // 1. If there are any indices resolving to the given source index
    // 2. That each concrete index is valid (in terms of mappings, etc.)
    @Suppress("ReturnCount")
    suspend fun isSourceIndexValid(rollup: Rollup): RollupJobValidationResult {
        // TODO: Add some entry in metadata that will store index -> indexUUID for validated indices
        //  That way, we only need to validate indices that aren't in the metadata (covers cases where new index with same name was made)
        // Allow no indices, open and closed
        // Rolling up on closed indices will not be caught here
        val concreteIndices =
            indexNameExpressionResolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpand(), true, rollup.sourceIndex)
        if (concreteIndices.isEmpty()) return RollupJobValidationResult.Invalid("No indices found for [${rollup.sourceIndex}]")

        // Validate mappings for each concrete index resolved from the rollup source index
        concreteIndices.forEach { index ->
            when (val sourceIndexMappingResult = isSourceIndexMappingsValid(index, rollup)) {
                is RollupJobValidationResult.Valid -> {} // no-op if valid
                is RollupJobValidationResult.Invalid -> return sourceIndexMappingResult
                is RollupJobValidationResult.Failure -> return sourceIndexMappingResult
            }
        }

        return RollupJobValidationResult.Valid
    }

    @Suppress("ReturnCount", "ComplexMethod")
    private suspend fun isSourceIndexMappingsValid(index: String, rollup: Rollup): RollupJobValidationResult {
        try {
            val res = when (val getMappingsResult = getMappings(index)) {
                is GetMappingsResult.Success -> getMappingsResult.response
                is GetMappingsResult.Failure ->
                    return RollupJobValidationResult.Failure(getMappingsResult.message, getMappingsResult.cause)
            }

            val indexTypeMappings = res.mappings[index]
            if (indexTypeMappings.isEmpty) {
                return RollupJobValidationResult.Invalid("Source index [$index] mappings are empty, cannot validate the job.")
            }

            // Starting from 6.0.0 an index can only have one mapping type, but mapping type is still part of the APIs in 7.x, allowing users to
            // set a custom mapping type. As a result using first mapping type found instead of _DOC mapping type to validate
            val indexMappingSource = indexTypeMappings.first().value.sourceAsMap

            val issues = mutableSetOf<String>()
            // Validate source fields in dimensions
            rollup.dimensions.forEach { dimension ->
                if (!isFieldInMappings(dimension.sourceField, indexMappingSource))
                    issues.add("missing field ${dimension.sourceField}")

                when (dimension) {
                    is DateHistogram -> {
                        // TODO: Validate if field is date type: date, date_nanos?
                    }
                    is Histogram -> {
                        // TODO: Validate field types for histograms
                    }
                    is Terms -> {
                        // TODO: Validate field types for terms
                    }
                }
            }

            // Validate source fields in metrics
            rollup.metrics.forEach { metric ->
                if (!isFieldInMappings(metric.sourceField, indexMappingSource))
                    issues.add("missing field ${metric.sourceField}")

                // TODO: Validate field type for metrics,
                //  are all Numeric field types valid?
            }

            return if (issues.isEmpty()) {
                RollupJobValidationResult.Valid
            } else {
                RollupJobValidationResult.Invalid("Invalid mappings for index [$index] because $issues")
            }
        } catch (e: Exception) {
            val errorMessage = "Failed to validate the source index mappings"
            logger.error(errorMessage, e)
            return RollupJobValidationResult.Failure(errorMessage, e)
        }
    }

    /**
     * Checks to see if the given field name is in the mappings map.
     *
     * The field name can be a path in the format "field1.field2...fieldn" so each field
     * will be checked in the map to get the next level until all subfields are checked for,
     * in which case true is returned. If at any point any of the fields is not in the map, false is returned.
     */
    private fun isFieldInMappings(fieldName: String, mappings: Map<*, *>): Boolean {
        val field = getFieldFromMappings(fieldName, mappings)
        return field != null
    }

    private suspend fun jobExistsInRollupIndex(rollup: Rollup): RollupJobValidationResult {
        val res = when (val getMappingsResult = getMappings(rollup.targetIndex)) {
            is GetMappingsResult.Success -> getMappingsResult.response
            is GetMappingsResult.Failure ->
                return RollupJobValidationResult.Failure(getMappingsResult.message, getMappingsResult.cause)
        }

        val indexMapping: MappingMetadata = res.mappings[rollup.targetIndex][_DOC]

        return if (((indexMapping.sourceAsMap?.get(_META) as Map<*, *>?)?.get(ROLLUPS) as Map<*, *>?)?.containsKey(rollup.id) == true) {
            RollupJobValidationResult.Valid
        } else {
            RollupJobValidationResult.Invalid("Rollup job [${rollup.id}] does not exist in rollup index [${rollup.targetIndex}]")
        }
    }

    @Suppress("ReturnCount")
    private suspend fun getMappings(index: String): GetMappingsResult {
        val errorMessage = "Failed to get mappings for index [$index]"
        try {
            val req = GetMappingsRequest().indices(index)
            val res: GetMappingsResponse? = client.admin().indices().suspendUntil { getMappings(req, it) }
            return if (res == null) {
                GetMappingsResult.Failure(cause = IllegalStateException("GetMappingsResponse for index [$index] was null"))
            } else {
                GetMappingsResult.Success(res)
            }
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.error(errorMessage, unwrappedException)
            return GetMappingsResult.Failure(errorMessage, unwrappedException)
        } catch (e: Exception) {
            logger.error(errorMessage, e)
            return GetMappingsResult.Failure(errorMessage, e)
        }
    }

    fun indexExists(index: String): Boolean = clusterService.state().routingTable.hasIndex(index)

    // TODO: error handling - can RemoteTransportException happen here?
    // TODO: The use of the master transport action UpdateRollupMappingAction will prevent
    //   overwriting an existing rollup job _meta by checking for the job id
    //   but there is still a race condition if two jobs are added at the same time for the
    //   same target index. There is a small time window after get mapping and put mappings
    //   where they can both get the same mapping state and only add their own job, meaning one
    //   of the jobs won't be added to the target index _meta
    @Suppress("BlockingMethodInNonBlockingContext", "ReturnCount")
    private suspend fun updateRollupIndexMappings(rollup: Rollup): RollupJobValidationResult {
        val errorMessage = "Failed to update mappings of target index [${rollup.targetIndex}] with rollup job"
        try {
            val response = withContext(Dispatchers.IO) {
                val resp: AcknowledgedResponse = client.suspendUntil {
                    execute(UpdateRollupMappingAction.INSTANCE, UpdateRollupMappingRequest(rollup), it)
                }
                resp.isAcknowledged
            }

            if (!response) {
                // TODO: when this happens is it failure or invalid?
                logger.error("$errorMessage with no exception")
                return RollupJobValidationResult.Failure(errorMessage)
            }
            return RollupJobValidationResult.Valid
        } catch (e: Exception) {
            logger.error("$errorMessage because ", e)
            return RollupJobValidationResult.Failure(errorMessage, e)
        }
    }

    companion object {
        const val ROLLUPS = "rollups"
    }

    sealed class GetMappingsResult {
        data class Success(val response: GetMappingsResponse) : GetMappingsResult()
        data class Failure(val message: String = "An error occurred when getting mappings", val cause: Exception) : GetMappingsResult()
    }
}
