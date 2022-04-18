/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.spi.indexstatemanagement.addObject

/** Properties that will persist across steps of a single Action. Will be stored in the [ActionMetaData]. */
// TODO: Create namespaces to group properties together
data class ActionProperties(
    val maxNumSegments: Int? = null,
    val snapshotName: String? = null,
    val rollupId: String? = null,
    val hasRollupFailed: Boolean? = null,
    val shrinkActionProperties: ShrinkActionProperties? = null
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalInt(maxNumSegments)
        out.writeOptionalString(snapshotName)
        out.writeOptionalString(rollupId)
        out.writeOptionalBoolean(hasRollupFailed)
        out.writeOptionalWriteable(shrinkActionProperties)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (maxNumSegments != null) builder.field(Properties.MAX_NUM_SEGMENTS.key, maxNumSegments)
        if (snapshotName != null) builder.field(Properties.SNAPSHOT_NAME.key, snapshotName)
        if (rollupId != null) builder.field(Properties.ROLLUP_ID.key, rollupId)
        if (hasRollupFailed != null) builder.field(Properties.HAS_ROLLUP_FAILED.key, hasRollupFailed)
        if (shrinkActionProperties != null) builder.addObject(ShrinkActionProperties.SHRINK_ACTION_PROPERTIES, shrinkActionProperties, params)
        return builder
    }

    companion object {
        const val ACTION_PROPERTIES = "action_properties"

        fun fromStreamInput(si: StreamInput): ActionProperties {
            val maxNumSegments: Int? = si.readOptionalInt()
            val snapshotName: String? = si.readOptionalString()
            val rollupId: String? = si.readOptionalString()
            val hasRollupFailed: Boolean? = si.readOptionalBoolean()
            val shrinkActionProperties: ShrinkActionProperties? = si.readOptionalWriteable { ShrinkActionProperties.fromStreamInput(it) }
            return ActionProperties(maxNumSegments, snapshotName, rollupId, hasRollupFailed, shrinkActionProperties)
        }

        fun parse(xcp: XContentParser): ActionProperties {
            var maxNumSegments: Int? = null
            var snapshotName: String? = null
            var rollupId: String? = null
            var hasRollupFailed: Boolean? = null
            var shrinkActionProperties: ShrinkActionProperties? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    Properties.MAX_NUM_SEGMENTS.key -> maxNumSegments = xcp.intValue()
                    Properties.SNAPSHOT_NAME.key -> snapshotName = xcp.text()
                    Properties.ROLLUP_ID.key -> rollupId = xcp.text()
                    Properties.HAS_ROLLUP_FAILED.key -> hasRollupFailed = xcp.booleanValue()
                    ShrinkActionProperties.SHRINK_ACTION_PROPERTIES -> {
                        shrinkActionProperties = if (xcp.currentToken() == Token.VALUE_NULL) null else ShrinkActionProperties.parse(xcp)
                    }
                }
            }

            return ActionProperties(maxNumSegments, snapshotName, rollupId, hasRollupFailed, shrinkActionProperties)
        }
    }

    enum class Properties(val key: String) {
        MAX_NUM_SEGMENTS("max_num_segments"),
        SNAPSHOT_NAME("snapshot_name"),
        ROLLUP_ID("rollup_id"),
        HAS_ROLLUP_FAILED("has_rollup_failed")
    }
}
