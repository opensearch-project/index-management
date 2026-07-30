/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.fielddomain.AttemptPublishFieldDomainsStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

/**
 * ISM action that computes and publishes finalized index-level field domains for the managed concrete index.
 *
 * The execution step requires the index write block and refreshes the index before computing domains. Those
 * preconditions are part of the action contract because finalized field-domain metadata may be used by search
 * coordinators to skip shard groups.
 */
class PublishFieldDomainsAction(
    val fields: List<FieldDomainConfig>,
    index: Int,
) : Action(name, index) {
    init {
        require(fields.isNotEmpty()) { "PublishFieldDomainsAction fields must not be empty" }
    }

    private val attemptPublishFieldDomainsStep = AttemptPublishFieldDomainsStep(this)
    private val steps = listOf(attemptPublishFieldDomainsStep)

    override fun getStepToExecute(context: StepContext): Step = attemptPublishFieldDomainsStep

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.startArray(FIELDS_FIELD)
        fields.forEach { it.toXContent(builder, params) }
        builder.endArray()
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeList(fields)
        out.writeInt(actionIndex)
    }

    companion object {
        const val name = "publish_field_domains"
        const val FIELDS_FIELD = "fields"
    }
}
