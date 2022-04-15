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

data class StateMetaData(
    val name: String,
    val startTime: Long
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeLong(startTime)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder
            .field(NAME, name)
            .field(START_TIME, startTime)
    }

    fun getMapValueString(): String = Strings.toString(this, false, false)

    companion object {
        const val STATE = "state"

        fun fromStreamInput(si: StreamInput): StateMetaData {
            val name: String? = si.readString()
            val startTime: Long? = si.readLong()

            return StateMetaData(
                requireNotNull(name) { "$NAME is null" },
                requireNotNull(startTime) { "$START_TIME is null" }
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): StateMetaData? {
            val stateJsonString = map[STATE]
            return if (stateJsonString != null) {
                val inputStream = ByteArrayInputStream(stateJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        fun parse(xcp: XContentParser): StateMetaData {
            var name: String? = null
            var startTime: Long? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME -> name = xcp.text()
                    START_TIME -> startTime = xcp.longValue()
                }
            }

            return StateMetaData(
                requireNotNull(name) { "$NAME is null" },
                requireNotNull(startTime) { "$START_TIME is null" }
            )
        }
    }
}
