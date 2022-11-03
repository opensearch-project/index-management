/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.addObject
import org.opensearch.indexmanagement.indexstatemanagement.settings.LegacyOpenDistroManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.TOTAL_MANAGED_INDICES
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ValidationResult
import java.io.IOException

open class ExplainResponse : ActionResponse, ToXContentObject {

    // TODO refactor these lists usage to map
    val indexNames: List<String>
    val indexPolicyIDs: List<String?>
    val indexMetadatas: List<ManagedIndexMetaData?>
    val totalManagedIndices: Int
    val enabledState: Map<String, Boolean>
    val policies: Map<String, Policy>
    val validationResults: List<ValidationResult?>

    @Suppress("LongParameterList")
    constructor(
        indexNames: List<String>,
        indexPolicyIDs: List<String?>,
        indexMetadatas: List<ManagedIndexMetaData?>,
        totalManagedIndices: Int,
        enabledState: Map<String, Boolean>,
        policies: Map<String, Policy>,
        validationResults: List<ValidationResult?>
    ) : super() {
        this.indexNames = indexNames
        this.indexPolicyIDs = indexPolicyIDs
        this.indexMetadatas = indexMetadatas
        this.totalManagedIndices = totalManagedIndices
        this.enabledState = enabledState
        this.policies = policies
        this.validationResults = validationResults
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indexNames = sin.readStringList(),
        indexPolicyIDs = sin.readStringList(),
        indexMetadatas = sin.readList { ManagedIndexMetaData.fromStreamInput(it) },
        totalManagedIndices = sin.readInt(),
        enabledState = sin.readMap() as Map<String, Boolean>,
        policies = sin.readMap(StreamInput::readString, ::Policy),
        validationResults = sin.readList { ValidationResult.fromStreamInput(it) }
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indexNames)
        out.writeStringCollection(indexPolicyIDs)
        out.writeCollection(indexMetadatas)
        out.writeInt(totalManagedIndices)
        out.writeMap(enabledState)
        out.writeMap(
            policies,
            { _out, key -> _out.writeString(key) },
            { _out, policy -> policy.writeTo(_out) }
        )
        out.writeCollection(validationResults)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        indexNames.forEachIndexed { ind, name ->
            builder.startObject(name)
            builder.field(ManagedIndexSettings.POLICY_ID.key, indexPolicyIDs[ind])
            builder.field(LegacyOpenDistroManagedIndexSettings.POLICY_ID.key, indexPolicyIDs[ind])
            indexMetadatas[ind]?.toXContent(builder, ToXContent.EMPTY_PARAMS)
            builder.field("enabled", enabledState[name])
            policies[name]?.let { builder.field(Policy.POLICY_TYPE, it, XCONTENT_WITHOUT_TYPE_AND_USER) }
            if (validationResults[ind] != null) {
                builder.addObject(ValidationResult.VALIDATE, validationResults[ind], params, true)
            }
            builder.endObject()
        }
        builder.field(TOTAL_MANAGED_INDICES, totalManagedIndices)
        return builder.endObject()
    }
}
