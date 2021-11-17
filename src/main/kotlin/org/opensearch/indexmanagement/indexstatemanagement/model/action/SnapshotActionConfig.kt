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
import org.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.script.ScriptService
import java.io.IOException

data class SnapshotActionConfig(
    val repository: String,
    val snapshot: String,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.SNAPSHOT, index) {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
            .startObject(ActionType.SNAPSHOT.type)
            .field(REPOSITORY_FIELD, repository)
            .field(SNAPSHOT_FIELD, snapshot)
        return builder.endObject().endObject()
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = SnapshotAction(clusterService, scriptService, client, managedIndexMetaData, this)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        repository = sin.readString(),
        snapshot = sin.readString(),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(repository)
        out.writeString(snapshot)
        out.writeInt(index)
    }

    companion object {
        const val REPOSITORY_FIELD = "repository"
        const val SNAPSHOT_FIELD = "snapshot"
        const val INCLUDE_GLOBAL_STATE = "include_global_state"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): SnapshotActionConfig {
            var repository: String? = null
            var snapshot: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    REPOSITORY_FIELD -> repository = xcp.text()
                    SNAPSHOT_FIELD -> snapshot = xcp.text()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in SnapshotActionConfig.")
                }
            }

            return SnapshotActionConfig(
                repository = requireNotNull(repository) { "SnapshotActionConfig repository must be specified" },
                snapshot = requireNotNull(snapshot) { "SnapshotActionConfig snapshot must be specified" },
                index = index
            )
        }
    }
}
