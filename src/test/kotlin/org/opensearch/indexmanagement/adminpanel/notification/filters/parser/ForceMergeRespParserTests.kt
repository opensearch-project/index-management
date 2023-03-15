/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filters.parser

import org.junit.Assert
import org.junit.Before
import org.opensearch.OpenSearchException
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse
import org.opensearch.action.support.DefaultShardOperationFailedException
import org.opensearch.action.support.broadcast.BroadcastResponse
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.xcontent.DeprecationHandler
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.Index
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.indexmanagement.adminpanel.notification.filter.parser.ForceMergeRespParser
import org.opensearch.indexmanagement.snapshotmanagement.toJsonString
import org.opensearch.tasks.Task
import org.opensearch.tasks.TaskId
import org.opensearch.test.OpenSearchTestCase

class ForceMergeRespParserTests : OpenSearchTestCase() {

    private lateinit var task: Task

    @Before
    fun setup() {
        task = Task(1, "transport", ReindexAction.NAME, "", TaskId.EMPTY_TASK_ID, mapOf())
    }

    fun `test build message for completion`() {
        val xContentParser = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS,
            "{\"_shards\":{\"total\":10,\"successful\":10,\"failed\":0}}"
        )

        val response = ForceMergeResponse.fromXContent(xContentParser)
        val request = ForceMergeRequest("test-index-1")
        val parser = ForceMergeRespParser(request)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "force_merge for index [test-index-1] has completed."
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
        val parser = ForceMergeRespParser(request)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "force_merge for index [test-index-1] has completed with errors. Error details: OpenSearchException[OpenSearch exception [type=exception, reason=shard is not available]]"
        )
    }
}
