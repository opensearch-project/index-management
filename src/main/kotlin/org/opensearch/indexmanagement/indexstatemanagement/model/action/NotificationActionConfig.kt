/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.Action
import org.opensearch.indexmanagement.indexstatemanagement.action.NotificationAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Destination
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import java.io.IOException

data class NotificationActionConfig(
    val destination: Destination,
    val messageTemplate: Script,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.NOTIFICATION, index) {

    init {
        require(messageTemplate.lang == MUSTACHE) { "Notification message template must be a mustache script" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params).startObject(ActionType.NOTIFICATION.type)
        builder.field(DESTINATION_FIELD, destination)
            .field(MESSAGE_TEMPLATE_FIELD, messageTemplate)
            .endObject()
            .endObject()
        return builder
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = NotificationAction(clusterService, scriptService, client, settings, managedIndexMetaData, this)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        destination = Destination(sin),
        messageTemplate = Script(sin),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        destination.writeTo(out)
        messageTemplate.writeTo(out)
        out.writeInt(index)
    }

    companion object {
        const val DESTINATION_FIELD = "destination"
        const val MESSAGE_TEMPLATE_FIELD = "message_template"
        const val MUSTACHE = "mustache"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): NotificationActionConfig {
            var destination: Destination? = null
            var messageTemplate: Script? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    DESTINATION_FIELD -> destination = Destination.parse(xcp)
                    MESSAGE_TEMPLATE_FIELD -> messageTemplate = Script.parse(xcp, Script.DEFAULT_TEMPLATE_LANG)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in NotificationActionConfig.")
                }
            }

            return NotificationActionConfig(
                destination = requireNotNull(destination) { "NotificationActionConfig destination is null" },
                messageTemplate = requireNotNull(messageTemplate) { "NotificationActionConfig message template is null" },
                index = index
            )
        }
    }
}
