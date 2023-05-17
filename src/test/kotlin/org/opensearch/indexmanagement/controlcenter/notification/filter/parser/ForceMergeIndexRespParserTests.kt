/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.junit.Assert
import org.opensearch.OpenSearchException
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse
import org.opensearch.action.support.DefaultShardOperationFailedException
import org.opensearch.action.support.broadcast.BroadcastResponse
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.xcontent.DeprecationHandler
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.Index
import org.opensearch.indexmanagement.snapshotmanagement.toJsonString

class ForceMergeIndexRespParserTests : BaseRespParserTests() {

    fun `test build message for completion`() {
        val xContentParser = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS,
            "{\"_shards\":{\"total\":10,\"successful\":10,\"failed\":0}}"
        )

        val response = ForceMergeResponse.fromXContent(xContentParser)
        val request = ForceMergeRequest("test-index-1")
        val parser = ForceMergeIndexRespParser(request, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "The force merge job on test-cluster/test-index-1 has completed."
        )
    }

    fun `test build message for failure`() {
        val ex = OpenSearchException("shard is not available")
        ex.index = Index("test-index-1", "uuid-1")
        val resp = BroadcastResponse(
            2, 1, 1,
            arrayListOf(DefaultShardOperationFailedException(ex))
        )

        val xContentParser = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS,
            resp.toJsonString()
        )

        val response = ForceMergeResponse.fromXContent(xContentParser)
        val request = ForceMergeRequest("test-index-1")
        val parser = ForceMergeIndexRespParser(request, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "The force merge job on test-cluster/test-index-1 has failed: OpenSearchException[OpenSearch exception [type=exception, reason=shard is not available]]"
        )
    }
}
