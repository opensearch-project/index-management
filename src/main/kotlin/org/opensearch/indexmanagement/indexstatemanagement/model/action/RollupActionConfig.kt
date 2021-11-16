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
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.indexmanagement.indexstatemanagement.action.Action
import org.opensearch.indexmanagement.indexstatemanagement.action.RollupAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.rollup.model.ISMRollup
import org.opensearch.script.ScriptService
import java.io.IOException

class RollupActionConfig(
    val ismRollup: ISMRollup,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.ROLLUP, index) {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
            .startObject(ActionType.ROLLUP.type)
            .field(ISM_ROLLUP_FIELD, ismRollup)
            .endObject()
            .endObject()
        return builder
    }

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = RollupAction(clusterService, client, managedIndexMetaData, this)

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(ismRollup = ISMRollup(sin), index = sin.readInt())

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        ismRollup.writeTo(out)
        out.writeInt(actionIndex)
    }

    companion object {
        const val ISM_ROLLUP_FIELD = "ism_rollup"
        var ismRollup: ISMRollup? = null

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, actionIndex: Int): RollupActionConfig {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ISM_ROLLUP_FIELD -> ismRollup = ISMRollup.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in RollupActionConfig.")
                }
            }

            return RollupActionConfig(
                ismRollup = requireNotNull(ismRollup) { "RollupActionConfig rollup is null" },
                index = actionIndex
            )
        }
    }
}
