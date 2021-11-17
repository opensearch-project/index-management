/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.settings.LegacyOpenDistroManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import java.io.IOException

class ExplainAllResponse : ExplainResponse, ToXContentObject {

    val totalManagedIndices: Int
    val enabledState: Map<String, Boolean>

    constructor(
        indexNames: List<String>,
        indexPolicyIDs: List<String?>,
        indexMetadatas: List<ManagedIndexMetaData?>,
        totalManagedIndices: Int,
        enabledState: Map<String, Boolean>
    ) : super(indexNames, indexPolicyIDs, indexMetadatas) {
        this.totalManagedIndices = totalManagedIndices
        this.enabledState = enabledState
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indexNames = sin.readStringList(),
        indexPolicyIDs = sin.readStringList(),
        indexMetadatas = sin.readList { ManagedIndexMetaData.fromStreamInput(it) },
        totalManagedIndices = sin.readInt(),
        enabledState = sin.readMap() as Map<String, Boolean>
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeInt(totalManagedIndices)
        out.writeMap(enabledState)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        indexNames.forEachIndexed { ind, name ->
            builder.startObject(name)
            builder.field(LegacyOpenDistroManagedIndexSettings.POLICY_ID.key, indexPolicyIDs[ind])
            builder.field(ManagedIndexSettings.POLICY_ID.key, indexPolicyIDs[ind])
            indexMetadatas[ind]?.toXContent(builder, ToXContent.EMPTY_PARAMS)
            builder.field("enabled", enabledState[name])
            builder.endObject()
        }
        builder.field("total_managed_indices", totalManagedIndices)
        return builder.endObject()
    }
}
