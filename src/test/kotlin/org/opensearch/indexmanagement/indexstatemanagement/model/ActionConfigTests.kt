/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.io.stream.InputStreamStreamInput
import org.opensearch.common.io.stream.OutputStreamStreamOutput
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionRetry
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionTimeout
import org.opensearch.indexmanagement.indexstatemanagement.model.action.IndexPriorityActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomAllocationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomForceMergeActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomIndexPriorityActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomNotificationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReplicaCountActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomRolloverActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomSnapshotActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomTimeValueObject
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.test.OpenSearchTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.test.assertFailsWith

class ActionConfigTests : OpenSearchTestCase() {

    fun `test invalid timeout for delete action config fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for invalid timeout") {
            ActionTimeout(timeout = TimeValue.parseTimeValue("invalidTimeout", "timeout_test"))
        }
    }

    fun `test action retry count of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for retry count less than 1") {
            ActionRetry(count = 0)
        }
    }

    fun `test rollover action minimum size of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for minSize less than 1") {
            randomRolloverActionConfig(minSize = ByteSizeValue.parseBytesSizeValue("0", "min_size_test"))
        }
    }

    fun `test rollover action minimum doc count of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for minDoc less than 1") {
            randomRolloverActionConfig(minDocs = 0)
        }
    }

    fun `test force merge action max num segments of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for maxNumSegments less than 1") {
            randomForceMergeActionConfig(maxNumSegments = 0)
        }
    }

    fun `test allocation action empty parameters fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for empty parameters") {
            randomAllocationActionConfig()
        }
    }

    fun `test rollover action round trip`() {
        roundTripActionConfig(randomRolloverActionConfig())
    }

    fun `test replica count action round trip`() {
        roundTripActionConfig(randomReplicaCountActionConfig())
    }

    fun `test force merge action round trip`() {
        roundTripActionConfig(randomForceMergeActionConfig())
    }

    fun `test notification action round trip`() {
        roundTripActionConfig(randomNotificationActionConfig())
    }

    fun `test snapshot action round trip`() {
        roundTripActionConfig(randomSnapshotActionConfig(snapshot = "snapshot", repository = "repository"))
    }

    fun `test index priority action round trip`() {
        roundTripActionConfig(randomIndexPriorityActionConfig())
    }

    fun `test allocation action round trip`() {
        roundTripActionConfig(randomAllocationActionConfig(require = mapOf("box_type" to "hot")))
    }

    fun `test action timeout and retry round trip`() {
        val builder = XContentFactory.jsonBuilder()
            .startObject()
            .field(ActionTimeout.TIMEOUT_FIELD, randomTimeValueObject().stringRep)
            .startObject(ActionRetry.RETRY_FIELD)
            .field(ActionRetry.COUNT_FIELD, 1)
            .field(ActionRetry.BACKOFF_FIELD, ActionRetry.Backoff.EXPONENTIAL)
            .field(ActionRetry.DELAY_FIELD, TimeValue.timeValueMinutes(1))
            .endObject()
            .startObject(ActionConfig.ActionType.INDEX_PRIORITY.type)
            .field(IndexPriorityActionConfig.INDEX_PRIORITY_FIELD, 10)
            .endObject()
            .endObject()

        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, builder.string())
        parser.nextToken()

        val actionConfig = ActionConfig.parse(parser, 1)
        roundTripActionConfig(actionConfig)
    }

    private fun roundTripActionConfig(expectedActionConfig: ActionConfig) {
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        expectedActionConfig.writeTo(osso)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))

        val actualActionConfig = ActionConfig.fromStreamInput(input)
        assertEquals(expectedActionConfig, actualActionConfig)
    }
}
