package org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils

data class ShrinkActionProperties(
    val nodeName: String? = null,
    val targetIndexName: String? = null,
    val targetNumShards: Int? = null,
    val lockPrimaryTerm: Long? = null,
    val lockSeqNo: Long? = null,
    val lockEpochSecond: Long? = null
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(nodeName)
        out.writeOptionalString(targetIndexName)
        out.writeOptionalInt(targetNumShards)
        out.writeOptionalLong(lockPrimaryTerm)
        out.writeOptionalLong(lockSeqNo)
        out.writeOptionalLong(lockEpochSecond)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (nodeName != null) builder.field(ShrinkProperties.SHRINK_NODE_NAME.key, nodeName)
        if (targetIndexName != null) builder.field(ShrinkProperties.SHRINK_TARGET_INDEX_NAME.key, targetIndexName)
        if (targetNumShards != null) builder.field(ShrinkProperties.SHRINK_TARGET_NUM_SHARDS.key, targetNumShards)
        if (lockSeqNo != null) builder.field(ShrinkProperties.SHRINK_LOCK_SEQ_NO.key, lockSeqNo)
        if (lockPrimaryTerm != null) builder.field(ShrinkProperties.SHRINK_LOCK_PRIMARY_TERM.key, lockPrimaryTerm)
        if (lockEpochSecond != null) builder.field(ShrinkProperties.SHRINK_LOCK_EPOCH_SECOND.key, lockEpochSecond)
        return builder
    }

    companion object {
        const val SHRINK_ACTION_PROPERTIES = "shrink_action_properties"

        fun fromStreamInput(si: StreamInput): ShrinkActionProperties {
            val nodeName: String? = si.readOptionalString()
            val targetIndexName: String? = si.readOptionalString()
            val targetNumShards: Int? = si.readOptionalInt()
            val lockPrimaryTerm: Long? = si.readOptionalLong()
            val lockSeqNo: Long? = si.readOptionalLong()
            val lockEpochSecond: Long? = si.readOptionalLong()

            return ShrinkActionProperties(nodeName, targetIndexName, targetNumShards, lockPrimaryTerm, lockSeqNo, lockEpochSecond)
        }

        fun parse(xcp: XContentParser): ShrinkActionProperties {
            var nodeName: String? = null
            var targetIndexName: String? = null
            var targetNumShards: Int? = null
            var lockPrimaryTerm: Long? = null
            var lockSeqNo: Long? = null
            var lockEpochSecond: Long? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ShrinkProperties.SHRINK_NODE_NAME.key -> nodeName = xcp.text()
                    ShrinkProperties.SHRINK_TARGET_INDEX_NAME.key -> targetIndexName = xcp.text()
                    ShrinkProperties.SHRINK_TARGET_NUM_SHARDS.key -> targetNumShards = xcp.intValue()
                    ShrinkProperties.SHRINK_LOCK_PRIMARY_TERM.key -> lockPrimaryTerm = xcp.longValue()
                    ShrinkProperties.SHRINK_LOCK_SEQ_NO.key -> lockSeqNo = xcp.longValue()
                    ShrinkProperties.SHRINK_LOCK_EPOCH_SECOND.key -> lockEpochSecond = xcp.longValue()
                }
            }

            return ShrinkActionProperties(nodeName, targetIndexName, targetNumShards, lockPrimaryTerm, lockSeqNo, lockEpochSecond)
        }
    }

    enum class ShrinkProperties(val key: String) {
        SHRINK_NODE_NAME("shrink_node_name"),
        SHRINK_TARGET_INDEX_NAME("shrink_target_index_name"),
        SHRINK_TARGET_NUM_SHARDS("shrink_target_num_shards"),
        SHRINK_LOCK_SEQ_NO("shrink_lock_seq_no"),
        SHRINK_LOCK_PRIMARY_TERM("shrink_lock_primary_term"),
        SHRINK_LOCK_EPOCH_SECOND("shrink_lock_epoch_second")
    }
}
