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
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.Locale

data class ValidationResult(
    val validationMessage: String,
    val validationStatus: Validate.ValidationStatus
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeString(validationMessage)
        validationStatus.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .field(VALIDATION_MESSAGE, validationMessage)
            .field(VALIDATION_STATUS, validationStatus.toString())
        return builder
    }

    fun getMapValueString(): String {
        return Strings.toString(this, false, false)
    }

    companion object {
        const val VALIDATE = "validate"
        const val VALIDATION_MESSAGE = "validation_message"
        const val VALIDATION_STATUS = "validation_status"

        fun fromStreamInput(si: StreamInput): ValidationResult {
            val validationMessage: String? = si.readString()
            val validationStatus: Validate.ValidationStatus? = Validate.ValidationStatus.read(si)

            return ValidationResult(
                requireNotNull(validationMessage) { "$VALIDATION_MESSAGE is null" },
                requireNotNull(validationStatus) { "$VALIDATION_STATUS is null" }
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): ValidationResult? {
            val stepJsonString = map[VALIDATE]
            return if (stepJsonString != null) {
                val inputStream = ByteArrayInputStream(stepJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        fun parse(xcp: XContentParser): ValidationResult {
            var validationMessage: String? = null
            var validationStatus: Validate.ValidationStatus? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    VALIDATION_MESSAGE -> validationMessage = xcp.text()
                    VALIDATION_STATUS -> validationStatus = Validate.ValidationStatus.valueOf(xcp.text().uppercase(Locale.ROOT))
                }
            }

            return ValidationResult(
                requireNotNull(validationMessage) { "$VALIDATION_MESSAGE is null" },
                requireNotNull(validationStatus) { "$VALIDATION_STATUS is null" }
            )
        }
    }
}
