/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.ALIASES_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.FORCE_UNSAFE_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.MAX_SHARD_SIZE_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.NUM_NEW_SHARDS_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.PERCENTAGE_OF_SOURCE_SHARDS_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.TARGET_INDEX_SUFFIX_FIELD
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ShrinkActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val numNewShards = sin.readOptionalInt()
        val maxShardSize = sin.readOptionalWriteable(::ByteSizeValue)
        val percentageOfSourceShards = sin.readOptionalDouble()
        val targetIndexSuffix = sin.readOptionalString()
        val aliases = if (sin.readBoolean()) sin.readList(::Alias) else null
        val forceUnsafe = sin.readOptionalBoolean()
        val index = sin.readInt()

        return ShrinkAction(numNewShards, maxShardSize, percentageOfSourceShards, targetIndexSuffix, aliases, forceUnsafe, index)
    }

    @Suppress("NestedBlockDepth")
    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var numNewShards: Int? = null
        var maxShardSize: ByteSizeValue? = null
        var percentageOfSourceShards: Double? = null
        var targetIndexSuffix: String? = null
        var aliases: List<Alias>? = null
        var forceUnsafe: Boolean? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                NUM_NEW_SHARDS_FIELD -> numNewShards = xcp.intValue()
                MAX_SHARD_SIZE_FIELD -> maxShardSize = ByteSizeValue.parseBytesSizeValue(xcp.text(), MAX_SHARD_SIZE_FIELD)
                PERCENTAGE_OF_SOURCE_SHARDS_FIELD -> percentageOfSourceShards = xcp.doubleValue()
                TARGET_INDEX_SUFFIX_FIELD -> targetIndexSuffix = xcp.text()
                ALIASES_FIELD -> {
                    if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                        aliases = mutableListOf()
                        when (xcp.currentToken()) {
                            XContentParser.Token.START_OBJECT -> {
                                while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                                    aliases.add(Alias.fromXContent(xcp))
                                }
                            }
                            else -> ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        }
                    }
                }
                FORCE_UNSAFE_FIELD -> forceUnsafe = xcp.booleanValue()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ShrinkAction.")
            }
        }

        return ShrinkAction(numNewShards, maxShardSize, percentageOfSourceShards, targetIndexSuffix, aliases, forceUnsafe, index)
    }

    override fun getActionType(): String {
        return ShrinkAction.name
    }
}
