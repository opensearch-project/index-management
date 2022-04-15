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
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData.Companion.NAME
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData.Companion.START_TIME
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.Locale

data class StepMetaData(
    val name: String,
    val startTime: Long,
    val stepStatus: Step.StepStatus
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeLong(startTime)
        stepStatus.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .field(NAME, name)
            .field(START_TIME, startTime)
            .field(STEP_STATUS, stepStatus.toString())

        return builder
    }

    fun getMapValueString(): String {
        return Strings.toString(this, false, false)
    }

    companion object {
        const val STEP = "step"
        const val STEP_STATUS = "step_status"

        fun fromStreamInput(si: StreamInput): StepMetaData {
            val name: String? = si.readString()
            val startTime: Long? = si.readLong()
            val stepStatus: Step.StepStatus? = Step.StepStatus.read(si)

            return StepMetaData(
                requireNotNull(name) { "$NAME is null" },
                requireNotNull(startTime) { "$START_TIME is null" },
                requireNotNull(stepStatus) { "$STEP_STATUS is null" }
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): StepMetaData? {
            val stepJsonString = map[STEP]
            return if (stepJsonString != null) {
                val inputStream = ByteArrayInputStream(stepJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        fun parse(xcp: XContentParser): StepMetaData {
            var name: String? = null
            var startTime: Long? = null
            var stepStatus: Step.StepStatus? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME -> name = xcp.text()
                    START_TIME -> startTime = xcp.longValue()
                    STEP_STATUS -> stepStatus = Step.StepStatus.valueOf(xcp.text().uppercase(Locale.ROOT))
                }
            }

            return StepMetaData(
                requireNotNull(name) { "$NAME is null" },
                requireNotNull(startTime) { "$START_TIME is null" },
                requireNotNull(stepStatus) { "$STEP_STATUS is null" }
            )
        }
    }
}
