/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.restore.AttemptRestoreStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ConvertIndexToRemoteAction(
    val repository: String,
    val snapshot: String,
    index: Int,
) : Action(name, index) {

    companion object {
        const val name = "convert_index_to_remote"
        const val REPOSITORY_FIELD = "repository"
        const val SNAPSHOT_FIELD = "snapshot"
    }

    private val attemptRestoreStep = AttemptRestoreStep(this)

    private val steps = listOf(attemptRestoreStep)

    override fun getStepToExecute(context: StepContext): Step = attemptRestoreStep

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(REPOSITORY_FIELD, repository)
        builder.field(SNAPSHOT_FIELD, snapshot)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeString(repository)
        out.writeString(snapshot)
        out.writeInt(actionIndex)
    }
}
