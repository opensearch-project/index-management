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
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

data class PolicyRetryInfoMetaData(
    val failed: Boolean,
    val consumedRetries: Int
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(failed)
        out.writeInt(consumedRetries)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder
            .field(FAILED, failed)
            .field(CONSUMED_RETRIES, consumedRetries)
    }

    fun getMapValueString(): String = Strings.toString(this, false, false)

    companion object {
        const val RETRY_INFO = "retry_info"
        const val FAILED = "failed"
        const val CONSUMED_RETRIES = "consumed_retries"

        fun fromStreamInput(si: StreamInput): PolicyRetryInfoMetaData {
            val failed: Boolean? = si.readBoolean()
            val consumedRetries: Int? = si.readInt()

            return PolicyRetryInfoMetaData(
                requireNotNull(failed) { "$FAILED is null" },
                requireNotNull(consumedRetries) { "$CONSUMED_RETRIES is null" }
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): PolicyRetryInfoMetaData? {
            val stateJsonString = map[RETRY_INFO]
            return if (stateJsonString != null) {
                val inputStream = ByteArrayInputStream(stateJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        fun parse(xcp: XContentParser): PolicyRetryInfoMetaData {
            var failed: Boolean? = null
            var consumedRetries: Int? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    FAILED -> failed = xcp.booleanValue()
                    CONSUMED_RETRIES -> consumedRetries = xcp.intValue()
                }
            }

            return PolicyRetryInfoMetaData(
                requireNotNull(failed) { "$FAILED is null" },
                requireNotNull(consumedRetries) { "$CONSUMED_RETRIES is null" }
            )
        }
    }
}
