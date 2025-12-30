/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.ALIASES_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.FORCE_UNSAFE_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.MAX_SHARD_SIZE_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.NUM_NEW_SHARDS_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.PERCENTAGE_OF_SOURCE_SHARDS_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.SWITCH_ALIASES
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.TARGET_INDEX_TEMPLATE_FIELD
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser
import org.opensearch.script.Script

class ShrinkActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val numNewShards = sin.readOptionalInt()
        val maxShardSize = sin.readOptionalWriteable(::ByteSizeValue)
        val percentageOfSourceShards = sin.readOptionalDouble()
        val targetIndexTemplate = if (sin.readBoolean()) Script(sin) else null
        val aliases = if (sin.readBoolean()) sin.readList(::Alias) else null
        val switchAliases = sin.readBoolean()
        val forceUnsafe = sin.readOptionalBoolean()
        val index = sin.readInt()

        return ShrinkAction(numNewShards, maxShardSize, percentageOfSourceShards, targetIndexTemplate, aliases, switchAliases, forceUnsafe, index)
    }

    @Suppress("NestedBlockDepth")
    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var numNewShards: Int? = null
        var maxShardSize: ByteSizeValue? = null
        var percentageOfSourceShards: Double? = null
        var targetIndexTemplate: Script? = null
        var aliases: List<Alias>? = null
        var switchAliases = false
        var forceUnsafe: Boolean? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                NUM_NEW_SHARDS_FIELD -> numNewShards = xcp.intValue()

                MAX_SHARD_SIZE_FIELD -> maxShardSize = ByteSizeValue.parseBytesSizeValue(xcp.text(), MAX_SHARD_SIZE_FIELD)

                PERCENTAGE_OF_SOURCE_SHARDS_FIELD -> percentageOfSourceShards = xcp.doubleValue()

                TARGET_INDEX_TEMPLATE_FIELD -> targetIndexTemplate = Script.parse(xcp, Script.DEFAULT_TEMPLATE_LANG)

                ALIASES_FIELD -> {
                    if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        aliases = mutableListOf()
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp)
                            aliases.add(Alias.fromXContent(xcp))
                            ensureExpectedToken(XContentParser.Token.END_OBJECT, xcp.nextToken(), xcp)
                        }
                    }
                }

                SWITCH_ALIASES -> switchAliases = xcp.booleanValue()

                FORCE_UNSAFE_FIELD -> forceUnsafe = xcp.booleanValue()

                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ShrinkAction.")
            }
        }

        return ShrinkAction(numNewShards, maxShardSize, percentageOfSourceShards, targetIndexTemplate, aliases, switchAliases, forceUnsafe, index)
    }

    override fun getActionType(): String = ShrinkAction.name
}
