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
import org.opensearch.indexmanagement.indexstatemanagement.action.ReplicaCountAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.script.ScriptService
import java.io.IOException

data class ReplicaCountActionConfig(
    val numOfReplicas: Int,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.REPLICA_COUNT, index) {

    init {
        require(numOfReplicas >= 0) { "ReplicaCountActionConfig number_of_replicas value must be a non-negative number" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params).startObject(ActionType.REPLICA_COUNT.type)
        builder.field(NUMBER_OF_REPLICAS_FIELD, numOfReplicas)
        return builder.endObject().endObject()
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = ReplicaCountAction(clusterService, client, managedIndexMetaData, this)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        numOfReplicas = sin.readInt(),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeInt(numOfReplicas)
        out.writeInt(index)
    }

    companion object {
        const val NUMBER_OF_REPLICAS_FIELD = "number_of_replicas"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): ReplicaCountActionConfig {
            var numOfReplicas: Int? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NUMBER_OF_REPLICAS_FIELD -> numOfReplicas = xcp.intValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ReplicaCountActionConfig.")
                }
            }

            return ReplicaCountActionConfig(
                numOfReplicas = requireNotNull(numOfReplicas) { "$NUMBER_OF_REPLICAS_FIELD is null" },
                index = index
            )
        }
    }
}
