/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.util

import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.client.Client
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.rollup.action.get.GetRollupsResponse
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.transform.action.get.GetTransformsResponse
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder

fun getJobs(
    client: Client,
    searchSourceBuilder: SearchSourceBuilder,
    listener: ActionListener<ActionResponse>,
    scheduledJobType: String,
    contentParser: (b: BytesReference) -> XContentParser = ::contentParser
) {
    val searchRequest = SearchRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
    client.search(
        searchRequest,
        object : ActionListener<SearchResponse> {
            override fun onResponse(response: SearchResponse) {
                val totalJobs = response.hits.totalHits?.value ?: 0

                if (response.shardFailures.isNotEmpty()) {
                    val failure = response.shardFailures.reduce { s1, s2 -> if (s1.status().status > s2.status().status) s1 else s2 }
                    listener.onFailure(OpenSearchStatusException("Get $scheduledJobType failed on some shards", failure.status(), failure.cause))
                } else {
                    try {
                        val jobs = response.hits.hits.map {
                            contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, getParser(scheduledJobType))
                        }
                        listener.onResponse(populateResponse(scheduledJobType, jobs, RestStatus.OK, totalJobs.toInt()))
                    } catch (e: Exception) {
                        listener.onFailure(
                            OpenSearchStatusException(
                                "Failed to parse $scheduledJobType",
                                RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e)
                            )
                        )
                    }
                }
            }

            override fun onFailure(e: Exception) = listener.onFailure(e)
        }
    )
}

private fun populateResponse(
    jobType: String,
    jobs: List<Any>,
    status: RestStatus,
    totalJobs: Int
): ActionResponse {
    return when (jobType) {
        Rollup.ROLLUP_TYPE -> GetRollupsResponse(jobs as List<Rollup>, totalJobs, status)
        Transform.TRANSFORM_TYPE -> GetTransformsResponse(jobs as List<Transform>, totalJobs, status)
        else -> {
            throw OpenSearchStatusException("Unknown scheduled job type", RestStatus.INTERNAL_SERVER_ERROR)
        }
    }
}

private fun getParser(jobType: String): (XContentParser, String, Long, Long) -> Any {
    return when (jobType) {
        Transform.TRANSFORM_TYPE -> Transform.Companion::parse
        Rollup.ROLLUP_TYPE -> Rollup.Companion::parse
        else -> {
            throw OpenSearchStatusException("Unknown scheduled job type", RestStatus.INTERNAL_SERVER_ERROR)
        }
    }
}

private fun contentParser(bytesReference: BytesReference): XContentParser {
    return XContentHelper.createParser(
        NamedXContentRegistry.EMPTY,
        LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON
    )
}
