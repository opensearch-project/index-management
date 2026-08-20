/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainCalculatorRegistry
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainConfig
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

/**
 * Parser for the ISM publish_field_domains action.
 */
class PublishFieldDomainsActionParser : ActionParser() {
    private val calculatorRegistry = FieldDomainCalculatorRegistry.defaultRegistry()

    override fun fromStreamInput(sin: StreamInput): Action {
        val fields = sin.readList(::FieldDomainConfig)
        val index = sin.readInt()
        validateSupportedTypes(fields)
        return PublishFieldDomainsAction(fields, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        val fields = mutableListOf<FieldDomainConfig>()

        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                PublishFieldDomainsAction.FIELDS_FIELD -> {
                    ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                    while (xcp.nextToken() != Token.END_ARRAY) {
                        fields.add(FieldDomainConfig.parse(xcp))
                    }
                }

                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in PublishFieldDomainsAction.")
            }
        }

        validateSupportedTypes(fields)
        return PublishFieldDomainsAction(fields, index)
    }

    override fun getActionType(): String = PublishFieldDomainsAction.name

    private fun validateSupportedTypes(fields: List<FieldDomainConfig>) {
        val unsupportedType = fields.firstOrNull { calculatorRegistry.get(it.type) == null }?.type ?: return
        throw IllegalArgumentException("No field-domain calculator registered for type [$unsupportedType]")
    }
}
