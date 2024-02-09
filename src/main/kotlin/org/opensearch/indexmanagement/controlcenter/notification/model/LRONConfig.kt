/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.model

import org.opensearch.commons.authuser.User
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.tasks.TaskId
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.indexmanagement.controlcenter.notification.util.WITH_PRIORITY
import org.opensearch.indexmanagement.controlcenter.notification.util.validateTaskIdAndActionName
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import org.opensearch.indexmanagement.opensearchapi.optionalUserField
import org.opensearch.indexmanagement.util.NO_ID
import java.io.IOException

data class LRONConfig(
    val lronCondition: LRONCondition,
    val taskId: TaskId?,
    val actionName: String?,
    val channels: List<Channel>?,
    val user: User?,
    val priority: Int?,
) : ToXContentObject, Writeable {
    init {
        validateTaskIdAndActionName(taskId, actionName)
        if (lronCondition.isEnabled()) {
            require(!channels.isNullOrEmpty()) { "Enabled LRONConfig must contain at least one channel" }
        }
    }

    fun toXContent(builder: XContentBuilder): XContentBuilder {
        return toXContent(builder, ToXContent.EMPTY_PARAMS)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(LRON_CONFIG_FIELD)
        builder.field(LRONCondition.LRON_CONDITION_FIELD, lronCondition)
        if (null != taskId) builder.field(TASK_ID_FIELD, taskId.toString())
        if (null != actionName) builder.field(ACTION_NAME_FIELD, actionName)
        if (params.paramAsBoolean(WITH_USER, true)) builder.optionalUserField(USER_FIELD, user)
        if (null != channels) {
            builder.startArray(CHANNELS_FIELD)
                .also { channels.forEach { channel -> channel.toXContent(it, params) } }
                .endArray()
        }
        if (params.paramAsBoolean(WITH_PRIORITY, true)) builder.field(PRIORITY_FIELD, priority)
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        return builder.endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        lronCondition = LRONCondition(sin),
        taskId = if (sin.readBoolean()) {
            TaskId(sin.readString())
        } else {
            null
        },
        actionName = sin.readOptionalString(),
        channels = if (sin.readBoolean()) {
            sin.readList(::Channel)
        } else {
            null
        },
        user = sin.readOptionalWriteable(::User),
        priority = sin.readOptionalInt(),
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        lronCondition.writeTo(out)
        if (null != taskId) {
            out.writeBoolean(true)
            out.writeString(taskId.toString())
        } else {
            out.writeBoolean(false)
        }
        out.writeOptionalString(actionName)
        if (null != channels) {
            out.writeBoolean(true)
            out.writeList(channels)
        } else {
            out.writeBoolean(false)
        }
        out.writeOptionalWriteable(user)
        out.writeOptionalInt(priority)
    }

    companion object {
        const val LRON_CONFIG_FIELD = "lron_config"
        const val LRON_CONFIG_FIELDS = "lron_configs"
        const val TASK_ID_FIELD = "task_id"
        const val ACTION_NAME_FIELD = "action_name"
        const val CHANNELS_FIELD = "channels"
        const val USER_FIELD = "user"
        const val PRIORITY_FIELD = "priority"
        const val CHANNEL_TITLE = "Long Running Operation Notification"

        /* to fit with ISM XContentParser.parseWithType function */
        @JvmStatic
        @Throws(IOException::class)
        @Suppress("UNUSED_PARAMETER")
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
        ): LRONConfig {
            return parse(xcp)
        }

        @JvmStatic
        @Suppress("MaxLineLength", "ComplexMethod", "NestedBlockDepth")
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): LRONConfig {
            var lronCondition = LRONCondition()
            var taskId: TaskId? = null
            var actionName: String? = null
            var channels: List<Channel>? = null
            var user: User? = null
            var priority: Int? = null

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    LRONCondition.LRON_CONDITION_FIELD -> lronCondition = LRONCondition.parse(xcp)
                    TASK_ID_FIELD ->
                        taskId =
                            if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else TaskId(xcp.text())
                    ACTION_NAME_FIELD ->
                        actionName =
                            if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else xcp.text()
                    CHANNELS_FIELD -> {
                        if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                            channels = mutableListOf()
                            ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                            while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                                channels.add(Channel.parse(xcp))
                            }
                        }
                    }
                    USER_FIELD -> user = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else User.parse(xcp)
                    PRIORITY_FIELD -> priority = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else xcp.intValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in LRONConfig.")
                }
            }

            return LRONConfig(
                lronCondition = lronCondition,
                taskId = taskId,
                actionName = actionName,
                channels = channels,
                user = user,
                priority = priority,
            )
        }
    }
}
