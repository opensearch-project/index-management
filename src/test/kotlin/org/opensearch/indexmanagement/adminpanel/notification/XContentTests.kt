/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification

import org.junit.Assert
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.util.PRIORITY_TASK_ID
import org.opensearch.indexmanagement.adminpanel.notification.util.getPriority
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.randomUser
import org.opensearch.script.Script
import org.opensearch.tasks.TaskId
import org.opensearch.test.OpenSearchTestCase
import java.util.Random

class XContentTests : OpenSearchTestCase() {

    fun `test lronConfig parsing`() {
        val xContentType = XContentType.values().random()
        val lronConfig = randomLRONConfig(enabled = true)
        Assert.assertEquals(
            buildMessage("lron_config", lronConfig, xContentType),
            lronConfig,
            parsedItem(lronConfig, xContentType, LRONConfig.Companion::parse))
    }

    fun `test lronConfigResponse`() {
        val lronConfig = LRONConfig(
            enabled = true,
            taskId = TaskId("node_123",456),
            actionName = "indices:admin/resize",
            channels = listOf(Channel("channel123"),Channel("channel456")),
            user = randomUser(),
            priority = PRIORITY_TASK_ID
        )
    }

    private fun <T : ToXContent> buildMessage(
        itemType: String,
        item: T,
        xContentType: XContentType
    ): String{
        return "$itemType toXContent test failed. xContentType: ${xContentType.subtype()}. " +
                "${item.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)}"
    }

    private fun <T : ToXContent> parsedItem(
        item: T,
        xContentType: XContentType,
        parser:(xcp: XContentParser) -> T
    ): T{
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
        return parser(xcp)
    }
}
