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
import org.opensearch.indexmanagement.indexstatemanagement.action.IndexPriorityAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.script.ScriptService
import java.io.IOException

data class IndexPriorityActionConfig(
    val indexPriority: Int,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.INDEX_PRIORITY, index) {

    init {
        require(indexPriority >= 0) { "IndexPriorityActionConfig index_priority value must be a non-negative number" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params).startObject(ActionType.INDEX_PRIORITY.type)
        builder.field(INDEX_PRIORITY_FIELD, indexPriority)
        return builder.endObject().endObject()
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = IndexPriorityAction(clusterService, client, managedIndexMetaData, this)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indexPriority = sin.readInt(),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeInt(indexPriority)
        out.writeInt(index)
    }

    companion object {
        const val INDEX_PRIORITY_FIELD = "priority"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): IndexPriorityActionConfig {
            var indexPriority: Int? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    INDEX_PRIORITY_FIELD -> indexPriority = xcp.intValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in IndexPriorityActionConfig.")
                }
            }

            return IndexPriorityActionConfig(
                indexPriority = requireNotNull(indexPriority) { "$INDEX_PRIORITY_FIELD is null" },
                index = index
            )
        }
    }
}
