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
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.randomAllocationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomCloseActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomForceMergeActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomIndexPriorityActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomNotificationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomOpenActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReadOnlyActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReadWriteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReplicaCountActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomRolloverActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomRollupActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomSnapshotActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomTimeValueObject
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout
import org.opensearch.test.OpenSearchTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.test.assertFailsWith

class ActionTests : OpenSearchTestCase() {

    fun `test invalid timeout for delete action fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for invalid timeout") {
            ActionTimeout(timeout = TimeValue.parseTimeValue("invalidTimeout", "timeout_test"))
        }
    }

    fun `test action retry count of -1 fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for retry count less than 1") {
            ActionRetry(count = -1)
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

    // TODO: fixme - enable the test
    private fun `test force merge action max num segments of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for maxNumSegments less than 1") {
            randomForceMergeActionConfig(maxNumSegments = 0)
        }
    }

    // TODO: fixme - enable the test
    private fun `test allocation action empty parameters fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for empty parameters") {
            randomAllocationActionConfig()
        }
    }

    fun `test set read write action round trip`() {
        roundTripAction(randomReadWriteActionConfig())
    }

    fun `test set read only action round trip`() {
        roundTripAction(randomReadOnlyActionConfig())
    }

    fun `test rollover action round trip`() {
        roundTripAction(randomRolloverActionConfig())
    }

    fun `test rollup action round trip`() {
        roundTripAction(randomRollupActionConfig())
    }

    fun `test replica count action round trip`() {
        roundTripAction(randomReplicaCountActionConfig())
    }

    // TODO: fixme - enable the test
    private fun `test force merge action round trip`() {
        roundTripAction(randomForceMergeActionConfig())
    }

    // TODO: fixme - enable the test
    private fun `test notification action round trip`() {
        roundTripAction(randomNotificationActionConfig())
    }

    // TODO: fixme - enable the test
    private fun `test snapshot action round trip`() {
        roundTripAction(randomSnapshotActionConfig(snapshot = "snapshot", repository = "repository"))
    }

    // TODO: fixme - enable the test
    private fun `test index priority action round trip`() {
        roundTripAction(randomIndexPriorityActionConfig())
    }

    // TODO: fixme - enable the test
    private fun `test allocation action round trip`() {
        roundTripAction(randomAllocationActionConfig(require = mapOf("box_type" to "hot")))
    }

    fun `test close action round trip`() {
        roundTripAction(randomCloseActionConfig())
    }

    fun `test open action round trip`() {
        roundTripAction(randomOpenActionConfig())
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
            .startObject(DeleteAction.name)
            .endObject()
            .endObject()

        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, builder.string())
        parser.nextToken()

        val action = ISMActionsParser.instance.parse(parser, 1)
        roundTripAction(action)
    }

    private fun roundTripAction(expectedAction: Action) {
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        expectedAction.writeTo(osso)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))

        val actualAction = ISMActionsParser.instance.fromStreamInput(input)
        assertEquals(expectedAction.convertToMap(), actualAction.convertToMap())
    }
}
