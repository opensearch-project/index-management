/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class RolloverAction(
    val minSize: ByteSizeValue?,
    val minDocs: Long?,
    val minAge: TimeValue?,
    index: Int
) : Action(name, index) {

    override fun getStepToExecute(context: StepContext): Step {
        TODO("Not yet implemented")
    }

    override fun getSteps(): List<Step> {
        TODO("Not yet implemented")
    }

    companion object {
        const val name = "rollover"
        const val MIN_SIZE_FIELD = "min_size"
        const val MIN_DOC_COUNT_FIELD = "min_doc_count"
        const val MIN_INDEX_AGE_FIELD = "min_index_age"
    }
}
