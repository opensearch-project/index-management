/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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
import org.opensearch.commons.utils.stringList
import org.opensearch.indexmanagement.indexstatemanagement.action.Action
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.script.ScriptService
import java.io.IOException

data class ShrinkActionConfig(
    val numNewShards: Int?,
    val maxShardSize: String?,
    val percentageDecrease: Int?,
    val targetIndexSuffix: String?,
    val aliases: List<String>?,
    val forceUnsafe: Boolean?,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.SHRINK, index) {

    init {
        val maxShardNotNull = if (maxShardSize != null) 1 else 0
        val percentageDecreaseNotNull = if (percentageDecrease != null) 1 else 0
        val numNewShardsNotNull = if (numNewShards != null) 1 else 0
        val numSet = maxShardNotNull + percentageDecreaseNotNull + numNewShardsNotNull
        require(numSet == 1) { "Exactly one of percentage_decrease, max_shard_size, or num_new must be specified" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
            .startObject(ActionType.SHRINK.type)
        // Check null because builder can't add null values
        if (numNewShards != null) builder.field(NUM_NEW_SHARDS_FIELD, numNewShards)
        if (maxShardSize != null) builder.field(MAX_SHARD_SIZE_FIELD, maxShardSize)
        if (percentageDecrease != null) builder.field(PERCENTAGE_DECREASE_FIELD, percentageDecrease)
        if (targetIndexSuffix != null) builder.field(TARGET_INDEX_SUFFIX_FIELD, targetIndexSuffix)
        if (aliases != null) builder.field(ALIASES_FIELD, aliases)
        if (forceUnsafe != null) builder.field(FORCE_UNSAFE_FIELD, forceUnsafe)
        return builder.endObject().endObject()
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = ShrinkAction(clusterService, client, managedIndexMetaData, this)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        numNewShards = sin.readOptionalInt(),
        maxShardSize = sin.readOptionalString(),
        percentageDecrease = sin.readOptionalInt(),
        targetIndexSuffix = sin.readOptionalString(),
        aliases = sin.readOptionalStringList(),
        forceUnsafe = sin.readOptionalBoolean(),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeOptionalInt(numNewShards)
        out.writeOptionalString(maxShardSize)
        out.writeOptionalInt(percentageDecrease)
        out.writeOptionalString(targetIndexSuffix)
        out.writeOptionalStringCollection(aliases)
        out.writeOptionalBoolean(forceUnsafe)
        out.writeInt(index)
    }

    companion object {
        const val NUM_NEW_SHARDS_FIELD = "num_new_shards"
        const val PERCENTAGE_DECREASE_FIELD = "percentage_decrease"
        const val MAX_SHARD_SIZE_FIELD = "max_shards_size"
        const val TARGET_INDEX_SUFFIX_FIELD = "target_index_suffix"
        const val ALIASES_FIELD = "aliases"
        const val FORCE_UNSAFE_FIELD = "force_unsafe"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): ShrinkActionConfig {
            var numNewShards: Int? = null
            var maxShardSize: String? = null
            var percentageDecrease: Int? = null
            var targetIndexSuffix: String? = null
            var aliases: List<String>? = null
            var forceUnsafe: Boolean? = null
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    NUM_NEW_SHARDS_FIELD -> numNewShards = xcp.intValue()
                    MAX_SHARD_SIZE_FIELD -> maxShardSize = xcp.textOrNull()
                    PERCENTAGE_DECREASE_FIELD -> percentageDecrease = xcp.intValue()
                    TARGET_INDEX_SUFFIX_FIELD -> targetIndexSuffix = xcp.textOrNull()
                    ALIASES_FIELD -> aliases = xcp.stringList()
                    FORCE_UNSAFE_FIELD -> forceUnsafe = xcp.booleanValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ShrinkActionConfig.")
                }
            }
            return ShrinkActionConfig(numNewShards, maxShardSize, percentageDecrease, targetIndexSuffix, aliases, forceUnsafe, index)
        }
    }
}
