/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:Suppress("TopLevelPropertyNaming", "MatchingDeclarationName")
package org.opensearch.indexmanagement.indexstatemanagement.util

import org.apache.logging.log4j.Logger
import org.opensearch.OpenSearchParseException
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.logging.DeprecationLogger
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.index.Index
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.rest.RestRequest
import java.lang.Exception
import java.time.Instant

const val WITH_TYPE = "with_type"
const val WITH_USER = "with_user"
val XCONTENT_WITHOUT_TYPE = ToXContent.MapParams(mapOf(WITH_TYPE to "false"))
val XCONTENT_WITHOUT_USER = ToXContent.MapParams(mapOf(WITH_USER to "false"))
val XCONTENT_WITHOUT_TYPE_AND_USER = ToXContent.MapParams(mapOf(WITH_TYPE to "false", WITH_USER to "false"))

const val FAILURES = "failures"
const val FAILED_INDICES = "failed_indices"
const val UPDATED_INDICES = "updated_indices"
const val TOTAL_MANAGED_INDICES = "total_managed_indices"

const val ISM_TEMPLATE_FIELD = "policy.ism_template"
const val MANAGED_INDEX_FIELD = "managed_index"
const val MANAGED_INDEX_NAME_KEYWORD_FIELD = "$MANAGED_INDEX_FIELD.name.keyword"
const val MANAGED_INDEX_INDEX_FIELD = "$MANAGED_INDEX_FIELD.index"
const val MANAGED_INDEX_INDEX_UUID_FIELD = "$MANAGED_INDEX_FIELD.index_uuid"

const val DEFAULT_JOB_SORT_FIELD = MANAGED_INDEX_INDEX_FIELD
const val DEFAULT_POLICY_SORT_FIELD = "policy.policy_id.keyword"

const val SHOW_POLICY_QUERY_PARAM = "show_policy"
const val DEFAULT_EXPLAIN_SHOW_POLICY = false

const val SHOW_VALIDATE_ACTION = "validate_action"
const val DEFAULT_EXPLAIN_VALIDATE_ACTION = false

const val INDEX_HIDDEN = "index.hidden"
const val INDEX_NUMBER_OF_SHARDS = "index.number_of_shards"
const val INDEX_NUMBER_OF_REPLICAS = "index.number_of_replicas"

const val TYPE_PARAM_KEY = "type"
const val DEFAULT_INDEX_TYPE = "_default"

fun buildInvalidIndexResponse(builder: XContentBuilder, failedIndices: List<FailedIndex>) {
    if (failedIndices.isNotEmpty()) {
        builder.field(FAILURES, true)
        builder.startArray(FAILED_INDICES)
        for (failedIndex in failedIndices) {
            failedIndex.toXContent(builder, ToXContent.EMPTY_PARAMS)
        }
        builder.endArray()
    } else {
        builder.field(FAILURES, false)
        builder.startArray(FAILED_INDICES).endArray()
    }
}

data class FailedIndex(val name: String, val uuid: String, val reason: String) : Writeable, ToXContentFragment {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(INDEX_NAME_FIELD, name)
            .field(INDEX_UUID_FIELD, uuid)
            .field(REASON_FIELD, reason)
        return builder.endObject()
    }

    companion object {
        const val INDEX_NAME_FIELD = "index_name"
        const val INDEX_UUID_FIELD = "index_uuid"
        const val REASON_FIELD = "reason"
    }

    constructor(sin: StreamInput) : this(
        name = sin.readString(),
        uuid = sin.readString(),
        reason = sin.readString()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeString(uuid)
        out.writeString(reason)
    }
}

/**
 * Gets the XContentBuilder for partially updating a [ManagedIndexConfig]'s ChangePolicy
 */
fun getPartialChangePolicyBuilder(
    changePolicy: ChangePolicy?
): XContentBuilder {
    val builder = XContentFactory.jsonBuilder()
        .startObject()
        .startObject(ManagedIndexConfig.MANAGED_INDEX_TYPE)
        .optionalTimeField(ManagedIndexConfig.LAST_UPDATED_TIME_FIELD, Instant.now())
        .field(ManagedIndexConfig.CHANGE_POLICY_FIELD, changePolicy)
    return builder.endObject().endObject()
}

/**
 * Removes the managed index metadata from the cluster state for the the provided indices.
 */
suspend fun removeClusterStateMetadatas(client: Client, logger: Logger, indices: List<Index>) {
    val request = UpdateManagedIndexMetaDataRequest(indicesToRemoveManagedIndexMetaDataFrom = indices)

    try {
        val response: AcknowledgedResponse = client.suspendUntil { execute(UpdateManagedIndexMetaDataAction.INSTANCE, request, it) }
        logger.debug("Cleaned cluster state metadata for $indices, ${response.isAcknowledged}")
    } catch (e: Exception) {
        logger.error("Failed to clean cluster state metadata for $indices")
    }
}

const val MASTER_TIMEOUT_DEPRECATED_MESSAGE =
    "Parameter [master_timeout] is deprecated and will be removed in 3.0. To support inclusive language, please use [cluster_manager_timeout] instead."
const val DUPLICATE_PARAMETER_ERROR_MESSAGE =
    "Please only use one of the request parameters [master_timeout, cluster_manager_timeout]."
fun parseClusterManagerTimeout(request: RestRequest, deprecationLogger: DeprecationLogger, restActionName: String): TimeValue {
    var timeout = request.paramAsTime("cluster_manager_timeout", ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT)

    if (request.hasParam("master_timeout")) {
        deprecationLogger.deprecate(restActionName + "_master_timeout_parameter", MASTER_TIMEOUT_DEPRECATED_MESSAGE)
        if (request.hasParam("cluster_manager_timeout")) {
            throw OpenSearchParseException(DUPLICATE_PARAMETER_ERROR_MESSAGE)
        }
        timeout = request.paramAsTime("master_timeout", timeout)
    }
    return timeout
}
