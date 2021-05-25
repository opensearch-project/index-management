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

package org.opensearch.indexmanagement.rollup.action.mapping

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.support.master.TransportMasterNodeAction
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import org.opensearch.indexmanagement.util.IndexUtils.Companion._META
import org.opensearch.indexmanagement.util._DOC
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.lang.Exception

class TransportUpdateRollupMappingAction @Inject constructor(
    threadPool: ThreadPool,
    clusterService: ClusterService,
    transportService: TransportService,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    val client: Client
) : TransportMasterNodeAction<UpdateRollupMappingRequest, AcknowledgedResponse>(
    UpdateRollupMappingAction.INSTANCE.name(),
    transportService,
    clusterService,
    threadPool,
    actionFilters,
    Writeable.Reader { UpdateRollupMappingRequest(it) },
    indexNameExpressionResolver
) {

    private val log = LogManager.getLogger(javaClass)

    override fun checkBlock(request: UpdateRollupMappingRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks.indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, arrayOf(request.rollup.targetIndex))
    }

    @Suppress("ReturnCount")
    override fun masterOperation(
        request: UpdateRollupMappingRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        val index = state.metadata.index(request.rollup.targetIndex)
        if (index == null) {
            log.debug("Could not find index [$index]")
            return listener.onFailure(IllegalStateException("Could not find index [$index]"))
        }
        val mappings = index.mapping()
        if (mappings == null) {
            log.debug("Could not find mapping for index [$index]")
            return listener.onFailure(IllegalStateException("Could not find mapping for index [$index]"))
        }
        val source = mappings.sourceAsMap
        if (source == null) {
            log.debug("Could not find source for index mapping [$index]")
            return listener.onFailure(IllegalStateException("Could not find source for index mapping [$index]"))
        }

        val rollup = XContentHelper.convertToMap(
            BytesReference.bytes(request.rollup.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)),
            false,
            XContentType.JSON
        ).v2()
        val metaMappings = mutableMapOf<String, Any>()
        // TODO: Clean this up
        val meta = source[_META]
        if (meta == null) {
            // TODO: Is schema_version always present?
            log.debug("Could not find meta mappings for index [$index], creating meta mappings")
            val rollupJobEntries = mapOf<String, Any>(request.rollup.id to rollup)
            val rollups = mapOf<String, Any>("rollups" to rollupJobEntries)
            metaMappings[_META] = rollups
        } else {
            val rollups = (meta as Map<*, *>)["rollups"]
            if (rollups == null) {
                log.debug("Could not find meta rollup mappings for index [$index], creating meta rollup mappings")
                val rollupJobEntries = mapOf<String, Any>(request.rollup.id to rollup)
                val updatedRollups = mapOf<String, Any>("rollups" to rollupJobEntries)
                metaMappings[_META] = updatedRollups
            } else {
                if ((rollups as Map<*, *>).containsKey(request.rollup.id)) {
                    log.debug("Meta rollup mappings already contain rollup ${request.rollup.id} for index [$index]")
                    return listener.onFailure(
                        IllegalStateException("Meta rollup mappings already contain rollup ${request.rollup.id} for index [$index]")
                    )
                }

                // In this case rollup mappings exists and there is no entry for request.rollup.id
                val rollupJobEntries = rollups.toMutableMap()
                rollupJobEntries[request.rollup.id] = rollup
                val updatedRollups = mapOf<String, Any>("rollups" to rollupJobEntries)
                metaMappings[_META] = updatedRollups
            }
        }

        // TODO: Does schema_version get overwritten?
        val putMappingRequest = PutMappingRequest(request.rollup.targetIndex).type(_DOC).source(metaMappings)
        client.admin().indices().putMapping(
            putMappingRequest,
            object : ActionListener<AcknowledgedResponse> {
                override fun onResponse(response: AcknowledgedResponse) {
                    listener.onResponse(response)
                }

                override fun onFailure(e: Exception) {
                    listener.onFailure(e)
                }
            }
        )
    }

    override fun read(sin: StreamInput): AcknowledgedResponse = AcknowledgedResponse(sin)

    override fun executor(): String = ThreadPool.Names.SAME
}
