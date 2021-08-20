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

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.Action
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.opensearchapi.aliasesField
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.script.ScriptService
import java.io.IOException

data class ShrinkActionConfig(
    val numNewShards: Int?,
    val maxShardSize: ByteSizeValue?,
    val percentageDecrease: Double?,
    val targetIndexSuffix: String?,
    val aliases: List<Alias>?,
    val forceUnsafe: Boolean?,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.SHRINK, index) {

    init {
        /*
        The numbers associated with each shard config are all k % mod 3 == 1.
        Because of the % 3 == 1 property, we can check if more than one shard configs are specified by
        modding the sum by 3. Any sum % 3 != 1 is a sum of more than one of the configs and thus invalid.
        We can then check the error message by checking the sum against each unique sum combination.
         */
        val maxShardNotNull = if (maxShardSize != null) MAX_SHARD_NOT_NULL else 0
        val percentageDecreaseNotNull = if (percentageDecrease != null) PERCENTAGE_DECREASE_NOT_NULL else 0
        val numNewShardsNotNull = if (numNewShards != null) NUM_SHARDS_NOT_NULL else 0
        val numSet = maxShardNotNull + percentageDecreaseNotNull + numNewShardsNotNull
        require(numSet % NUM_SHARD_CONFIGS == 1) {
            when (numSet) {
                MAX_SHARD_NOT_NULL + PERCENTAGE_DECREASE_NOT_NULL ->
                    "Cannot specify both maximum shard size and percentage decrease. Please pick one."
                MAX_SHARD_NOT_NULL + NUM_SHARDS_NOT_NULL ->
                    "Cannot specify both maximum shard size and number of new shards. Please pick one."
                PERCENTAGE_DECREASE_NOT_NULL + NUM_SHARDS_NOT_NULL ->
                    "Cannot specify both percentage decrease and number of new shards. Please pick one."
                MAX_SHARD_NOT_NULL + PERCENTAGE_DECREASE_NOT_NULL + NUM_SHARDS_NOT_NULL ->
                    "Cannot specify maximum shard size, percentage decrease, and number of new shards. Please pick one."
                // Never executes this code block.
                else -> ""
            }
        }
        if (percentageDecreaseNotNull != 0) {
            require(percentageDecrease!!.compareTo(0.0) == 1 && percentageDecrease.compareTo(1.0) == -1) {
                "Percentage decrease must be between 0.0 and 1.0 exclusively"
            }
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
            .startObject(ActionType.SHRINK.type)
        // Check null because builder can't add null values
        if (numNewShards != null) builder.field(NUM_NEW_SHARDS_FIELD, numNewShards)
        if (maxShardSize != null) builder.field(MAX_SHARD_SIZE_FIELD, maxShardSize.stringRep)
        if (percentageDecrease != null) builder.field(PERCENTAGE_DECREASE_FIELD, percentageDecrease)
        if (targetIndexSuffix != null) builder.field(TARGET_INDEX_SUFFIX_FIELD, targetIndexSuffix)
        if (aliases != null) { builder.aliasesField(aliases) }
        if (forceUnsafe != null) builder.field(FORCE_UNSAFE_FIELD, forceUnsafe)
        return builder.endObject().endObject()
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData,
        context: JobExecutionContext
    ): Action = ShrinkAction(clusterService, client, managedIndexMetaData, this, context)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        numNewShards = sin.readOptionalInt(),
        maxShardSize = sin.readOptionalWriteable(::ByteSizeValue),
        percentageDecrease = sin.readOptionalDouble(),
        targetIndexSuffix = sin.readOptionalString(),
        aliases = if (sin.readBoolean()) {
            sin.readList(::Alias)
        } else null,
        forceUnsafe = sin.readOptionalBoolean(),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeOptionalInt(numNewShards)
        out.writeOptionalWriteable(maxShardSize)
        out.writeOptionalDouble(percentageDecrease)
        out.writeOptionalString(targetIndexSuffix)
        if (aliases != null) {
            out.writeBoolean(true)
            out.writeList(aliases)
        } else {
            out.writeBoolean(false)
        }
        out.writeOptionalBoolean(forceUnsafe)
        out.writeInt(index)
    }

    companion object {
        const val NUM_NEW_SHARDS_FIELD = "num_new_shards"
        const val PERCENTAGE_DECREASE_FIELD = "percentage_decrease"
        const val MAX_SHARD_SIZE_FIELD = "max_shard_size"
        const val TARGET_INDEX_SUFFIX_FIELD = "target_index_suffix"
        const val ALIASES_FIELD = "aliases"
        const val FORCE_UNSAFE_FIELD = "force_unsafe"
        const val MAX_SHARD_NOT_NULL = 1
        const val PERCENTAGE_DECREASE_NOT_NULL = 4
        const val NUM_SHARDS_NOT_NULL = 7
        const val NUM_SHARD_CONFIGS = 3
        val logger: Logger = LogManager.getLogger(javaClass)

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): ShrinkActionConfig {
            var numNewShards: Int? = null
            var maxShardSize: ByteSizeValue? = null
            var percentageDecrease: Double? = null
            var targetIndexSuffix: String? = null
            var aliases: List<Alias>? = null
            var forceUnsafe: Boolean? = null
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    NUM_NEW_SHARDS_FIELD -> numNewShards = xcp.intValue()
                    MAX_SHARD_SIZE_FIELD -> maxShardSize = ByteSizeValue.parseBytesSizeValue(xcp.textOrNull(), MAX_SHARD_SIZE_FIELD)
                    PERCENTAGE_DECREASE_FIELD -> percentageDecrease = xcp.doubleValue()
                    TARGET_INDEX_SUFFIX_FIELD -> targetIndexSuffix = xcp.textOrNull()
                    ALIASES_FIELD -> {
                        if (xcp.currentToken() != Token.VALUE_NULL) {
                            aliases = mutableListOf()
                            when (xcp.currentToken()) {
                                Token.START_OBJECT -> {
                                    while (xcp.nextToken() != Token.END_OBJECT) {
                                        aliases.add(Alias.fromXContent(xcp))
                                    }
                                }
                                else -> ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                            }
                        }
                    }
                    FORCE_UNSAFE_FIELD -> forceUnsafe = xcp.booleanValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ShrinkActionConfig.")
                }
            }
            return ShrinkActionConfig(numNewShards, maxShardSize, percentageDecrease, targetIndexSuffix, aliases, forceUnsafe, index)
        }
    }
}
