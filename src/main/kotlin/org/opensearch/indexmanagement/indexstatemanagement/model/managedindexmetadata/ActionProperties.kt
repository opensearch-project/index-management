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

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken

/** Properties that will persist across steps of a single Action. Will be stored in the [ActionMetaData]. */
// TODO: Create namespaces to group properties together
data class ActionProperties(
    val maxNumSegments: Int? = null,
    val snapshotName: String? = null,
    val rollupId: String? = null,
    val hasRollupFailed: Boolean? = null,
    val shrinkNodeId: Int? = null,
    val shrinkTargetIndexName: String? = null,
    val shrinkNumShards: Int? = null
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalInt(maxNumSegments)
        out.writeOptionalString(snapshotName)
        out.writeOptionalString(rollupId)
        out.writeOptionalBoolean(hasRollupFailed)
        out.writeOptionalInt(shrinkNodeId)
        out.writeOptionalString(shrinkTargetIndexName)
        out.writeOptionalInt(shrinkNumShards)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (maxNumSegments != null) builder.field(Properties.MAX_NUM_SEGMENTS.key, maxNumSegments)
        if (snapshotName != null) builder.field(Properties.SNAPSHOT_NAME.key, snapshotName)
        if (rollupId != null) builder.field(Properties.ROLLUP_ID.key, rollupId)
        if (hasRollupFailed != null) builder.field(Properties.HAS_ROLLUP_FAILED.key, hasRollupFailed)
        if (shrinkNodeId != null) builder.field(Properties.SHRINK_NODE_ID.key, shrinkNodeId)
        if (shrinkTargetIndexName != null) builder.field(Properties.SHRINK_TARGET_INDEX_NAME.key, shrinkTargetIndexName)
        if (shrinkNumShards != null) builder.field(Properties.SHRINK_NUM_SHARDS.key, shrinkNumShards)
        return builder
    }

    companion object {
        const val ACTION_PROPERTIES = "action_properties"

        fun fromStreamInput(si: StreamInput): ActionProperties {
            val maxNumSegments: Int? = si.readOptionalInt()
            val snapshotName: String? = si.readOptionalString()
            val rollupId: String? = si.readOptionalString()
            val hasRollupFailed: Boolean? = si.readOptionalBoolean()
            val shrinkNodeId: Int? = si.readOptionalInt()
            val shrinkTargetIndexName: String? = si.readOptionalString()
            val shrinkNumShards: Int? = si.readOptionalInt()

            return ActionProperties(maxNumSegments, snapshotName, rollupId, hasRollupFailed, shrinkNodeId, shrinkTargetIndexName, shrinkNumShards)
        }

        fun parse(xcp: XContentParser): ActionProperties {
            var maxNumSegments: Int? = null
            var snapshotName: String? = null
            var rollupId: String? = null
            var hasRollupFailed: Boolean? = null
            var shrinkNodeId: Int? = null
            var shrinkTargetIndexName: String? = null
            var shrinkNumShards: Int? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    Properties.MAX_NUM_SEGMENTS.key -> maxNumSegments = xcp.intValue()
                    Properties.SNAPSHOT_NAME.key -> snapshotName = xcp.text()
                    Properties.ROLLUP_ID.key -> rollupId = xcp.text()
                    Properties.HAS_ROLLUP_FAILED.key -> hasRollupFailed = xcp.booleanValue()
                    Properties.SHRINK_NODE_ID.key -> shrinkNodeId = xcp.intValue()
                    Properties.SHRINK_TARGET_INDEX_NAME.key -> shrinkTargetIndexName = xcp.text()
                    Properties.SHRINK_NUM_SHARDS.key -> shrinkNumShards = xcp.intValue()
                }
            }

            return ActionProperties(maxNumSegments, snapshotName, rollupId, hasRollupFailed, shrinkNodeId, shrinkTargetIndexName, shrinkNumShards)
        }
    }

    enum class Properties(val key: String) {
        MAX_NUM_SEGMENTS("max_num_segments"),
        SNAPSHOT_NAME("snapshot_name"),
        ROLLUP_ID("rollup_id"),
        HAS_ROLLUP_FAILED("has_rollup_failed"),
        SHRINK_NODE_ID("shrink_node_id"),
        SHRINK_TARGET_INDEX_NAME("shrink_target_index_name"),
        SHRINK_NUM_SHARDS("shrink_num_shards")
    }
}
