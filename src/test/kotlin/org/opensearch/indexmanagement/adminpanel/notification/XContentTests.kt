/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification

import org.junit.Assert
import org.junit.BeforeClass
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigsResponse
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.util.PRIORITY_TASK_ID
import org.opensearch.indexmanagement.adminpanel.notification.util.getDocID
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.randomUser
import org.opensearch.tasks.TaskId
import org.opensearch.test.OpenSearchTestCase

class XContentTests : OpenSearchTestCase() {

    fun `test lronConfig parsing`() {
        Assert.assertEquals(
            buildMessage("lronConfig", XContentType.JSON),
            sampleLRONConfig,
            parsedItem(sampleLRONConfig, XContentType.JSON, LRONConfig.Companion::parse)
        )

        val xContentType = XContentType.values().random()
        val lronConfig = randomLRONConfig()
        Assert.assertEquals(
            buildMessage("lronConfig", xContentType),
            lronConfig,
            parsedItem(lronConfig, xContentType, LRONConfig.Companion::parse)
        )
    }

    fun `test lronConfigResponse`() {
        val responseString = sampleLRONConfigResponse
            .toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()
        /* we drop the user info and priority info in rest layer */
        assertEquals("lronConfigResponse toXcontent failed.", sampleExpectedJson, responseString)
    }

    fun `test getLRONConfigsResponse`() {
        val response = GetLRONConfigsResponse(
            listOf(sampleLRONConfigResponse, sampleLRONConfigResponse),
            totalNumber = 2,
            timedOut = false
        )
        val responseString = response.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()
        val expectedJSON = """
            {
              "lron_configs": [
                $sampleExpectedJson,
                $sampleExpectedJson
              ],
              "total_number": 2,
              "timed_out": false
            }
        """.replace("\\s".toRegex(), "")

        assertEquals("lronConfigResponse toXcontent failed.", expectedJSON, responseString)
    }

    private fun buildMessage(
        itemType: String,
        xContentType: XContentType
    ): String {
        return "$itemType toXContent test failed. xContentType: ${xContentType.subtype()}. "
    }

    private fun <T : ToXContent> parsedItem(
        item: T,
        xContentType: XContentType,
        parseWithTypeParser: (xcp: XContentParser, id: String, seqNo: Long, primaryTerm: Long) -> T
    ): T {
        val bytesReference = toShuffledXContent(
            item,
            xContentType.xContent().mediaType(),
            ToXContent.EMPTY_PARAMS,
            randomBoolean()
        )
        val xcp = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            bytesReference,
            xContentType.xContent().mediaType()
        )
        return xcp.parseWithType(parse = parseWithTypeParser)
    }

    companion object {
        lateinit var sampleLRONConfig: LRONConfig
        lateinit var sampleLRONConfigResponse: LRONConfigResponse
        lateinit var sampleExpectedJson: String

        @BeforeClass
        @JvmStatic
        fun setup() {
            sampleLRONConfig = LRONConfig(
                enabled = true,
                taskId = TaskId("node_123", 456L),
                actionName = "indices:admin/resize",
                channels = listOf(Channel("channel123"), Channel("channel456")),
                user = randomUser(),
                priority = PRIORITY_TASK_ID
            )
            sampleLRONConfigResponse = LRONConfigResponse(
                id = getDocID(sampleLRONConfig.taskId, sampleLRONConfig.actionName),
                version = 789L,
                primaryTerm = 123L,
                seqNo = 456L,
                lronConfig = sampleLRONConfig
            )
            sampleExpectedJson = """
            {
              "_id": "LRON:node_123:456",
              "_version": 789,
              "_primary_term": 123,
              "_seq_no": 456,
              "lron_config": {
                "enabled": true,
                "task_id": "node_123:456",
                "action_name": "indices:admin/resize",
                "channels": [
                  {
                    "id": "channel123"
                  },
                  {
                    "id": "channel456"
                  }
                ]
              }
            }
            """.replace("\\s".toRegex(), "")
        }
    }
}
