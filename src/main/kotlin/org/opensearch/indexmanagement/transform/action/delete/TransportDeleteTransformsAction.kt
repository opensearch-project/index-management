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

package org.opensearch.indexmanagement.transform.action.delete

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.rest.RestStatus
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportDeleteTransformsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<DeleteTransformsRequest, BulkResponse>(
    DeleteTransformsAction.NAME, transportService, actionFilters, ::DeleteTransformsRequest
) {

    override fun doExecute(task: Task, request: DeleteTransformsRequest, actionListener: ActionListener<BulkResponse>) {
        // TODO: if metadata id exists delete the metadata doc else just delete transform
        // Use Multi-Get Request
        val getRequest = MultiGetRequest()
        val includes = arrayOf(
            "${Transform.TRANSFORM_TYPE}.${Transform.ENABLED_FIELD}"
        )
        val fetchSourceContext = FetchSourceContext(true, includes, emptyArray())
        request.ids.forEach { id ->
            getRequest.add(MultiGetRequest.Item(INDEX_MANAGEMENT_INDEX, id).fetchSourceContext(fetchSourceContext))
        }

        client.multiGet(getRequest, object : ActionListener<MultiGetResponse> {
            override fun onResponse(response: MultiGetResponse) {
                try {
                    // response is failed only if managed index is not present
                    if (response.responses.first().isFailed) {
                        actionListener.onFailure(
                            OpenSearchStatusException(
                                "Cluster missing system index $INDEX_MANAGEMENT_INDEX, cannot execute the request", RestStatus.BAD_REQUEST
                            )
                        )
                        return
                    }

                    bulkDelete(response, request.ids, request.force, actionListener)
                } catch (e: Exception) {
                    actionListener.onFailure(e)
                }
            }

            override fun onFailure(e: Exception) = actionListener.onFailure(e)
        })
    }

    private fun bulkDelete(response: MultiGetResponse, ids: List<String>, forceDelete: Boolean, actionListener: ActionListener<BulkResponse>) {
        val enabledIDs = mutableListOf<String>()
        val notTransform = mutableListOf<String>()

        response.responses.forEach {
            if (it.response.isExists) {
                val source = it.response.source
                val enabled = (source["transform"] as Map<*, *>?)?.get("enabled") as Boolean?
                if (enabled == null) {
                    notTransform.add(it.id)
                }
                if (enabled == true && !forceDelete) {
                    enabledIDs.add(it.id)
                }
            }
        }

        if (notTransform.isNotEmpty()) {
            actionListener.onFailure(OpenSearchStatusException(
                "$notTransform IDs are not transforms!", RestStatus.BAD_REQUEST
            ))
            return
        }

        if (enabledIDs.isNotEmpty()) {
            actionListener.onFailure(OpenSearchStatusException(
                "$enabledIDs transform(s) are enabled, please disable them before deleting them or set force flag", RestStatus.CONFLICT
            ))
            return
        }

        val bulkDeleteRequest = BulkRequest()
        bulkDeleteRequest.refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        for (id in ids) {
            bulkDeleteRequest.add(DeleteRequest(INDEX_MANAGEMENT_INDEX, id))
        }

        client.bulk(bulkDeleteRequest, object : ActionListener<BulkResponse> {
            override fun onResponse(response: BulkResponse) {
                actionListener.onResponse(response)
            }

            override fun onFailure(e: Exception) = actionListener.onFailure(e)
        })
    }
}
