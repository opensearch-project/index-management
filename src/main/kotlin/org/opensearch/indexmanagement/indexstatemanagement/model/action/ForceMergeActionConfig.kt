/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.opensearch.indexmanagement.indexstatemanagement.action.Action
import org.opensearch.indexmanagement.indexstatemanagement.action.ForceMergeAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.script.ScriptService
import java.io.IOException

data class ForceMergeActionConfig(
    val maxNumSegments: Int,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.FORCE_MERGE, index) {

    init {
        require(maxNumSegments > 0) { "Force merge {$MAX_NUM_SEGMENTS_FIELD} must be greater than 0" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
            .startObject(ActionType.FORCE_MERGE.type)
            .field(MAX_NUM_SEGMENTS_FIELD, maxNumSegments)
            .endObject()
        return builder.endObject()
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = ForceMergeAction(clusterService, client, managedIndexMetaData, this)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        maxNumSegments = sin.readInt(),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeInt(maxNumSegments)
        out.writeInt(index)
    }

    companion object {
        const val MAX_NUM_SEGMENTS_FIELD = "max_num_segments"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): ForceMergeActionConfig {
            var maxNumSegments: Int? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    MAX_NUM_SEGMENTS_FIELD -> maxNumSegments = xcp.intValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ForceMergeActionConfig.")
                }
            }

            return ForceMergeActionConfig(
                requireNotNull(maxNumSegments) { "ForceMergeActionConfig maxNumSegments is null" },
                index
            )
        }
    }
}
