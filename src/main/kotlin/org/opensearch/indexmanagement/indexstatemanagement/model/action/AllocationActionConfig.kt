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
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.script.ScriptService
import java.io.IOException

data class AllocationActionConfig(
    val require: Map<String, String>,
    val include: Map<String, String>,
    val exclude: Map<String, String>,
    val waitFor: Boolean = false,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.ALLOCATION, index) {

    init {
        require(require.isNotEmpty() || include.isNotEmpty() || exclude.isNotEmpty()) { "At least one allocation parameter need to be specified." }
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = AllocationAction(clusterService, client, managedIndexMetaData, this)

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
            .startObject(ActionType.ALLOCATION.type)
        if (require.isNotEmpty()) builder.field(REQUIRE, require)
        if (include.isNotEmpty()) builder.field(INCLUDE, include)
        if (exclude.isNotEmpty()) builder.field(EXCLUDE, exclude)
        return builder.field(WAIT_FOR, waitFor)
            .endObject()
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        require = suppressWarning(sin.readMap()),
        include = suppressWarning(sin.readMap()),
        exclude = suppressWarning(sin.readMap()),
        waitFor = sin.readBoolean(),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeMap(require)
        out.writeMap(include)
        out.writeMap(exclude)
        out.writeBoolean(waitFor)
        out.writeInt(index)
    }

    companion object {
        const val REQUIRE = "require"
        const val INCLUDE = "include"
        const val EXCLUDE = "exclude"
        const val WAIT_FOR = "wait_for"

        @Suppress("UNCHECKED_CAST")
        fun suppressWarning(map: MutableMap<String?, Any?>?): MutableMap<String, String> {
            return map as MutableMap<String, String>
        }

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): AllocationActionConfig {
            val require: MutableMap<String, String> = mutableMapOf()
            val include: MutableMap<String, String> = mutableMapOf()
            val exclude: MutableMap<String, String> = mutableMapOf()
            var waitFor = false

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    REQUIRE -> assignObject(xcp, require)
                    INCLUDE -> assignObject(xcp, include)
                    EXCLUDE -> assignObject(xcp, exclude)
                    WAIT_FOR -> waitFor = xcp.booleanValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in AllocationActionConfig.")
                }
            }
            return AllocationActionConfig(require, include, exclude, waitFor, index)
        }

        private fun assignObject(xcp: XContentParser, objectMap: MutableMap<String, String>) {
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                objectMap[fieldName] = xcp.text()
            }
        }
    }
}
