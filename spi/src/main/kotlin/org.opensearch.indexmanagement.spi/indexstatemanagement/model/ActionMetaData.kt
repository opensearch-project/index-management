/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.model

import org.opensearch.common.Strings
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData.Companion.NAME
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData.Companion.START_TIME
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

data class ActionMetaData(
    val name: String,
    val startTime: Long?,
    val index: Int,
    val failed: Boolean,
    val consumedRetries: Int,
    val lastRetryTime: Long?,
    val actionProperties: ActionProperties?
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeOptionalLong(startTime)
        out.writeInt(index)
        out.writeBoolean(failed)
        out.writeInt(consumedRetries)
        out.writeOptionalLong(lastRetryTime)

        out.writeOptionalWriteable(actionProperties)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .field(NAME, name)
            .field(START_TIME, startTime)
            .field(INDEX, index)
            .field(FAILED, failed)
            .field(CONSUMED_RETRIES, consumedRetries)
            .field(LAST_RETRY_TIME, lastRetryTime)

        if (actionProperties != null) {
            builder.startObject(ActionProperties.ACTION_PROPERTIES)
            actionProperties.toXContent(builder, params)
            builder.endObject()
        }

        return builder
    }

    fun getMapValueString(): String {
        return Strings.toString(this, false, false)
    }

    companion object {
        const val ACTION = "action"
        const val INDEX = "index"
        const val FAILED = "failed"
        const val CONSUMED_RETRIES = "consumed_retries"
        const val LAST_RETRY_TIME = "last_retry_time"

        fun fromStreamInput(si: StreamInput): ActionMetaData {
            val name: String? = si.readString()
            val startTime: Long? = si.readOptionalLong()
            val index: Int? = si.readInt()
            val failed: Boolean? = si.readBoolean()
            val consumedRetries: Int? = si.readInt()
            val lastRetryTime: Long? = si.readOptionalLong()

            val actionProperties: ActionProperties? = si.readOptionalWriteable { ActionProperties.fromStreamInput(it) }

            return ActionMetaData(
                requireNotNull(name) { "$NAME is null" },
                startTime,
                requireNotNull(index) { "$INDEX is null" },
                requireNotNull(failed) { "$FAILED is null" },
                requireNotNull(consumedRetries) { "$CONSUMED_RETRIES is null" },
                lastRetryTime,
                actionProperties
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): ActionMetaData? {
            val actionJsonString = map[ACTION]
            return if (actionJsonString != null) {
                val inputStream = ByteArrayInputStream(actionJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        @Suppress("ComplexMethod")
        fun parse(xcp: XContentParser): ActionMetaData {
            var name: String? = null
            var startTime: Long? = null
            var index: Int? = null
            var failed: Boolean? = null
            var consumedRetries: Int? = null
            var lastRetryTime: Long? = null
            var actionProperties: ActionProperties? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME -> name = xcp.text()
                    START_TIME -> startTime = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else xcp.longValue()
                    INDEX -> index = xcp.intValue()
                    FAILED -> failed = xcp.booleanValue()
                    CONSUMED_RETRIES -> consumedRetries = xcp.intValue()
                    LAST_RETRY_TIME -> lastRetryTime = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else xcp.longValue()
                    ActionProperties.ACTION_PROPERTIES ->
                        actionProperties = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else ActionProperties.parse(xcp)
                }
            }

            return ActionMetaData(
                requireNotNull(name) { "$NAME is null" },
                startTime,
                requireNotNull(index) { "$INDEX is null" },
                requireNotNull(failed) { "$FAILED is null" },
                requireNotNull(consumedRetries) { "$CONSUMED_RETRIES is null" },
                lastRetryTime,
                actionProperties
            )
        }
    }
}
