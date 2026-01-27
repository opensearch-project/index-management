/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.cluster.routing.allocation.DiskThresholdSettings
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.io.stream.InputStreamStreamInput
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.NotificationAction
import org.opensearch.indexmanagement.indexstatemanagement.randomAllocationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.randomChannel
import org.opensearch.indexmanagement.indexstatemanagement.randomCloseActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomDeleteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomDestination
import org.opensearch.indexmanagement.indexstatemanagement.randomForceMergeActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomIndexPriorityActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomNotificationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomOpenActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReadOnlyActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReadWriteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReplicaCountActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomRestoreActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomRolloverActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomRollupActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.randomSnapshotActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomTemplateScript
import org.opensearch.indexmanagement.indexstatemanagement.randomTimeValueObject
import org.opensearch.indexmanagement.indexstatemanagement.util.getFreeBytesThresholdHigh
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout
import org.opensearch.test.OpenSearchTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.lang.Math.abs
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

    fun `test force merge action max num segments of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for maxNumSegments less than 1") {
            randomForceMergeActionConfig(maxNumSegments = 0)
        }
    }

    fun `test shrink action multiple shard options fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for multiple shard options used") {
            randomShrinkAction(3, randomByteSizeValue(), .30)
        }
    }

    fun `test allocation action empty parameters fails`() {
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

    fun `test notification having both channel and destination fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for notification using both destination and channel") {
            NotificationAction(destination = randomDestination(), channel = randomChannel(), messageTemplate = randomTemplateScript(), index = 0)
        }
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

    fun `test force merge action round trip`() {
        roundTripAction(randomForceMergeActionConfig())
    }

    fun `test notification action round trip`() {
        roundTripAction(randomNotificationActionConfig())
    }

    fun `test snapshot action round trip`() {
        roundTripAction(randomSnapshotActionConfig(snapshot = "snapshot", repository = "repository"))
    }

    fun `test index priority action round trip`() {
        roundTripAction(randomIndexPriorityActionConfig())
    }

    fun `test allocation action round trip`() {
        roundTripAction(
            randomAllocationActionConfig
            (
                require = mapOf("box_type" to "hot"),
                include = mapOf(randomAlphaOfLengthBetween(1, 10) to randomAlphaOfLengthBetween(1, 10)),
                exclude = mapOf(randomAlphaOfLengthBetween(1, 10) to randomAlphaOfLengthBetween(1, 10)),
            ),
        )
    }

    fun `test close action round trip`() {
        roundTripAction(randomCloseActionConfig())
    }

    fun `test open action round trip`() {
        roundTripAction(randomOpenActionConfig())
    }

    fun `test delete action round trip`() {
        roundTripAction(randomDeleteActionConfig())
    }

    fun `test shrink action round trip`() {
        roundTripAction(randomShrinkAction())
    }

    fun `test convert index to remote action round trip with defaults`() {
        roundTripAction(randomRestoreActionConfig())
    }

    fun `test convert index to remote action round trip with all fields`() {
        val convertAction = ConvertIndexToRemoteAction(
            repository = "test-repo",
            snapshot = "test-snapshot",
            includeAliases = true,
            ignoreIndexSettings = "index.refresh_interval,index.number_of_replicas",
            numberOfReplicas = 2,
            deleteOriginalIndex = true,
            index = 0,
        )
        roundTripAction(convertAction)
    }

    fun `test convert index to remote action round trip with includeAliases only`() {
        val convertAction = ConvertIndexToRemoteAction(
            repository = "test-repo",
            snapshot = "test-snapshot",
            includeAliases = true,
            ignoreIndexSettings = "",
            numberOfReplicas = 0,
            deleteOriginalIndex = false,
            index = 0,
        )
        roundTripAction(convertAction)
    }

    fun `test convert index to remote action round trip with ignoreIndexSettings only`() {
        val convertAction = ConvertIndexToRemoteAction(
            repository = "test-repo",
            snapshot = "test-snapshot",
            includeAliases = false,
            ignoreIndexSettings = "index.refresh_interval",
            numberOfReplicas = 0,
            deleteOriginalIndex = false,
            index = 0,
        )
        roundTripAction(convertAction)
    }

    fun `test convert index to remote action round trip with numberOfReplicas only`() {
        val convertAction = ConvertIndexToRemoteAction(
            repository = "test-repo",
            snapshot = "test-snapshot",
            includeAliases = false,
            ignoreIndexSettings = "",
            numberOfReplicas = 3,
            deleteOriginalIndex = false,
            index = 0,
        )
        roundTripAction(convertAction)
    }

    fun `test convert index to remote action round trip with deleteOriginalIndex only`() {
        val convertAction = ConvertIndexToRemoteAction(
            repository = "test-repo",
            snapshot = "test-snapshot",
            includeAliases = false,
            ignoreIndexSettings = "",
            numberOfReplicas = 0,
            deleteOriginalIndex = true,
            index = 0,
        )
        roundTripAction(convertAction)
    }

    fun `test convert index to remote action backward compatibility with older version`() {
        // Test that reading from an older version (before V_3_3_0) uses defaults for new fields
        val convertAction = ConvertIndexToRemoteAction(
            repository = "test-repo",
            snapshot = "test-snapshot",
            includeAliases = true,
            ignoreIndexSettings = "index.refresh_interval",
            numberOfReplicas = 2,
            deleteOriginalIndex = true,
            index = 0,
        )

        // Write with older version (should not write new fields)
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos).also {
            it.version = org.opensearch.Version.V_3_2_0 // Version before V_3_3_0
        }
        convertAction.writeTo(osso)

        // Read with older version (should use defaults)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray())).also {
            it.version = org.opensearch.Version.V_3_2_0
        }
        val parsedAction = ISMActionsParser.instance.fromStreamInput(input) as ConvertIndexToRemoteAction

        // Verify that new fields use defaults when reading from older version
        assertEquals("repository should match", "test-repo", parsedAction.repository)
        assertEquals("snapshot should match", "test-snapshot", parsedAction.snapshot)
        assertEquals("includeAliases should default to false", false, parsedAction.includeAliases)
        assertEquals("ignoreIndexSettings should default to empty", "", parsedAction.ignoreIndexSettings)
        assertEquals("numberOfReplicas should default to 0", 0, parsedAction.numberOfReplicas)
        assertEquals("deleteOriginalIndex should default to false", false, parsedAction.deleteOriginalIndex)
    }

    fun `test convert index to remote action forward compatibility with newer version`() {
        // Test that reading from a newer version includes all fields
        val convertAction = ConvertIndexToRemoteAction(
            repository = "test-repo",
            snapshot = "test-snapshot",
            includeAliases = true,
            ignoreIndexSettings = "index.refresh_interval",
            numberOfReplicas = 2,
            deleteOriginalIndex = true,
            index = 0,
        )

        // Write with newer version (should write all fields)
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos).also {
            it.version = org.opensearch.Version.V_3_3_0
        }
        convertAction.writeTo(osso)

        // Read with newer version (should read all fields)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray())).also {
            it.version = org.opensearch.Version.V_3_3_0
        }
        val parsedAction = ISMActionsParser.instance.fromStreamInput(input) as ConvertIndexToRemoteAction

        // Verify that all fields are preserved
        assertEquals("repository should match", "test-repo", parsedAction.repository)
        assertEquals("snapshot should match", "test-snapshot", parsedAction.snapshot)
        assertEquals("includeAliases should be true", true, parsedAction.includeAliases)
        assertEquals("ignoreIndexSettings should match", "index.refresh_interval", parsedAction.ignoreIndexSettings)
        assertEquals("numberOfReplicas should be 2", 2, parsedAction.numberOfReplicas)
        assertEquals("deleteOriginalIndex should be true", true, parsedAction.deleteOriginalIndex)
    }

    fun `test action timeout and retry round trip`() {
        val builder =
            XContentFactory.jsonBuilder()
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

    fun `test shrink disk threshold percentage settings`() {
        val rawPercentage = randomIntBetween(0, 100)
        val percentage = "$rawPercentage%"
        val settings =
            Settings.builder().put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.key, percentage)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.key, percentage)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.key, percentage).build()
        val clusterSettings = ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.map { it }.toSet())
        val totalNodeBytes = randomByteSizeValue().bytes
        val thresholdBytes = getFreeBytesThresholdHigh(clusterSettings, totalNodeBytes)
        val expectedThreshold: Long = ((1 - (rawPercentage.toDouble() / 100.0)) * totalNodeBytes).toLong()
        // To account for some rounding issues, allow an error of 1
        val approximatelyEqual = kotlin.math.abs(thresholdBytes - expectedThreshold) <= 1
        assertTrue("Free bytes threshold not being calculated correctly for percentage setting.", approximatelyEqual)
    }

    fun `test shrink disk threshold byte settings`() {
        val byteValue = randomByteSizeValue()
        val settings =
            Settings.builder().put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.key, byteValue)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.key, byteValue)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.key, byteValue).build()
        val clusterSettings = ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.map { it }.toSet())
        val thresholdBytes = getFreeBytesThresholdHigh(clusterSettings, randomByteSizeValue().bytes)
        assertEquals("Free bytes threshold not being calculated correctly for byte setting.", thresholdBytes, byteValue.bytes)
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
