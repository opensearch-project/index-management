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
import org.opensearch.common.xcontent.XContentParserUtils

data class ShrinkActionProperties(
    val nodeName: String,
    val targetIndexName: String,
    val targetNumShards: Int,
    val lockPrimaryTerm: Long,
    val lockSeqNo: Long,
    val lockEpochSecond: Long,
    val lockDurationSecond: Long,
    // Used to store the original index allocation and write block setting to reapply after shrink
    val originalIndexSettings: Map<String, String>
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeString(nodeName)
        out.writeString(targetIndexName)
        out.writeInt(targetNumShards)
        out.writeLong(lockPrimaryTerm)
        out.writeLong(lockSeqNo)
        out.writeLong(lockEpochSecond)
        out.writeLong(lockDurationSecond)
        out.writeMap(originalIndexSettings)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.field(ShrinkProperties.NODE_NAME.key, nodeName)
        builder.field(ShrinkProperties.TARGET_INDEX_NAME.key, targetIndexName)
        builder.field(ShrinkProperties.TARGET_NUM_SHARDS.key, targetNumShards)
        builder.field(ShrinkProperties.LOCK_SEQ_NO.key, lockSeqNo)
        builder.field(ShrinkProperties.LOCK_PRIMARY_TERM.key, lockPrimaryTerm)
        builder.field(ShrinkProperties.LOCK_EPOCH_SECOND.key, lockEpochSecond)
        builder.field(ShrinkProperties.LOCK_DURATION_SECOND.key, lockDurationSecond)
        builder.field(ShrinkProperties.ORIGINAL_INDEX_SETTINGS.key, originalIndexSettings)
        return builder
    }

    companion object {
        const val SHRINK_ACTION_PROPERTIES = "shrink_action_properties"

        fun fromStreamInput(si: StreamInput): ShrinkActionProperties {
            val nodeName: String = si.readString()
            val targetIndexName: String = si.readString()
            val targetNumShards: Int = si.readInt()
            val lockPrimaryTerm: Long = si.readLong()
            val lockSeqNo: Long = si.readLong()
            val lockEpochSecond: Long = si.readLong()
            val lockDurationSecond: Long = si.readLong()
            val originalIndexSettings: Map<String, String> = si.readMap({ it.readString() }, { it.readString() })

            return ShrinkActionProperties(
                nodeName, targetIndexName, targetNumShards, lockPrimaryTerm, lockSeqNo, lockEpochSecond, lockDurationSecond, originalIndexSettings
            )
        }

        fun parse(xcp: XContentParser): ShrinkActionProperties {
            var nodeName: String? = null
            var targetIndexName: String? = null
            var targetNumShards: Int? = null
            var lockPrimaryTerm: Long? = null
            var lockSeqNo: Long? = null
            var lockEpochSecond: Long? = null
            var lockDurationSecond: Long? = null
            var originalIndexSettings: Map<String, String>? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ShrinkProperties.NODE_NAME.key -> nodeName = xcp.text()
                    ShrinkProperties.TARGET_INDEX_NAME.key -> targetIndexName = xcp.text()
                    ShrinkProperties.TARGET_NUM_SHARDS.key -> targetNumShards = xcp.intValue()
                    ShrinkProperties.LOCK_PRIMARY_TERM.key -> lockPrimaryTerm = xcp.longValue()
                    ShrinkProperties.LOCK_SEQ_NO.key -> lockSeqNo = xcp.longValue()
                    ShrinkProperties.LOCK_EPOCH_SECOND.key -> lockEpochSecond = xcp.longValue()
                    ShrinkProperties.LOCK_DURATION_SECOND.key -> lockDurationSecond = xcp.longValue()
                    ShrinkProperties.ORIGINAL_INDEX_SETTINGS.key -> originalIndexSettings = xcp.mapStrings()
                }
            }

            return ShrinkActionProperties(
                requireNotNull(nodeName),
                requireNotNull(targetIndexName),
                requireNotNull(targetNumShards),
                requireNotNull(lockPrimaryTerm),
                requireNotNull(lockSeqNo),
                requireNotNull(lockEpochSecond),
                requireNotNull(lockDurationSecond),
                requireNotNull(originalIndexSettings)
            )
        }
    }

    enum class ShrinkProperties(val key: String) {
        NODE_NAME("node_name"),
        TARGET_INDEX_NAME("target_index_name"),
        TARGET_NUM_SHARDS("target_num_shards"),
        LOCK_SEQ_NO("lock_seq_no"),
        LOCK_PRIMARY_TERM("lock_primary_term"),
        LOCK_EPOCH_SECOND("lock_epoch_second"),
        LOCK_DURATION_SECOND("lock_duration_second"),
        ORIGINAL_INDEX_SETTINGS("original_index_settings")
    }
}
