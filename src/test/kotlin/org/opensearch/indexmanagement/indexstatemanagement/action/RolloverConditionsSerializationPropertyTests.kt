/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.Version
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.io.stream.InputStreamStreamInput
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.test.OpenSearchTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class RolloverConditionsSerializationPropertyTests : OpenSearchTestCase() {

    private val iterations = 100

    private fun randomGroup(): RolloverConditionGroup {
        while (true) {
            val minSize = if (randomBoolean()) ByteSizeValue(randomLongBetween(1L, 1_000_000L)) else null
            val minDocs = if (randomBoolean()) randomLongBetween(1L, 1_000_000L) else null
            val minAge = if (randomBoolean()) TimeValue.timeValueMillis(randomLongBetween(1L, 1_000_000L)) else null
            val minPrimaryShardSize = if (randomBoolean()) ByteSizeValue(randomLongBetween(1L, 1_000_000L)) else null
            if (listOfNotNull(minSize, minDocs, minAge, minPrimaryShardSize).isNotEmpty()) {
                return RolloverConditionGroup(minSize, minDocs, minAge, minPrimaryShardSize)
            }
        }
    }

    private fun randomGroupedAction(): RolloverAction = RolloverAction(
        minSize = null, minDocs = null, minAge = null, minPrimaryShardSize = null,
        copyAlias = randomBoolean(),
        preventEmptyRollover = randomBoolean(),
        conditionGroups = (1..randomIntBetween(1, 4)).map { randomGroup() },
        index = 0,
    )

    private fun randomFlatAction(): RolloverAction {
        while (true) {
            val minSize = if (randomBoolean()) ByteSizeValue(randomLongBetween(1L, 1_000_000L)) else null
            val minDocs = if (randomBoolean()) randomLongBetween(1L, 1_000_000L) else null
            val minAge = if (randomBoolean()) TimeValue.timeValueMillis(randomLongBetween(1L, 1_000_000L)) else null
            val minPrimaryShardSize = if (randomBoolean()) ByteSizeValue(randomLongBetween(1L, 1_000_000L)) else null
            if (listOfNotNull(minSize, minDocs, minAge, minPrimaryShardSize).isNotEmpty()) {
                return RolloverAction(
                    minSize = minSize, minDocs = minDocs, minAge = minAge, minPrimaryShardSize = minPrimaryShardSize,
                    copyAlias = randomBoolean(),
                    preventEmptyRollover = randomBoolean(),
                    index = 0,
                )
            }
        }
    }

    private fun randomAnyAction(): RolloverAction = if (randomBoolean()) randomGroupedAction() else randomFlatAction()

    private fun randomAtOrAboveTargetVersion(): Version = randomFrom(Version.V_3_7_0, Version.CURRENT)

    private fun randomBelowTargetVersion(): Version =
        randomFrom(Version.V_3_3_0, Version.V_3_4_0, Version.V_3_5_0, Version.V_3_6_0)

    private fun groupsEqual(a: List<RolloverConditionGroup>?, b: List<RolloverConditionGroup>?): Boolean = a == b

    private fun parseFromXContent(action: RolloverAction): RolloverAction {
        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        action.populateAction(builder, ToXContent.EMPTY_PARAMS)
        builder.endObject()
        val json = builder.string()
        val parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(), LoggingDeprecationHandler.INSTANCE, json,
        )
        parser.nextToken()
        return ISMActionsParser.instance.parse(parser, 0) as RolloverAction
    }

    private fun roundTripStream(action: RolloverAction, version: Version): RolloverAction {
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        osso.version = version
        action.writeTo(osso)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        input.version = version
        return ISMActionsParser.instance.fromStreamInput(input) as RolloverAction
    }

    fun `test XContent round-trip preserves form and conditions`() {
        repeat(iterations) {
            val action = randomAnyAction()
            val builder = XContentFactory.jsonBuilder()
            builder.startObject()
            action.populateAction(builder, ToXContent.EMPTY_PARAMS)
            builder.endObject()
            val json = builder.string()

            if (!action.conditionGroups.isNullOrEmpty()) {
                assertTrue("Grouped action XContent must contain any_of", json.contains("\"${RolloverAction.ANY_OF_FIELD}\""))
            }

            val parsed = parseFromXContent(action)
            assertTrue("conditionGroups must round-trip", groupsEqual(action.conditionGroups, parsed.conditionGroups))
            assertEquals("minSize must round-trip", action.minSize, parsed.minSize)
            assertEquals("minDocs must round-trip", action.minDocs, parsed.minDocs)
            assertEquals("minAge must round-trip", action.minAge, parsed.minAge)
            assertEquals("minPrimaryShardSize must round-trip", action.minPrimaryShardSize, parsed.minPrimaryShardSize)
        }
    }

    fun `test stream round-trip at or above target version preserves groups`() {
        repeat(iterations) {
            val action = randomGroupedAction()
            val version = randomAtOrAboveTargetVersion()
            val parsed = roundTripStream(action, version)
            assertTrue("Groups must round-trip at/above target version", groupsEqual(action.conditionGroups, parsed.conditionGroups))
            assertEquals("copyAlias must round-trip", action.copyAlias, parsed.copyAlias)
            assertEquals("preventEmptyRollover must round-trip", action.preventEmptyRollover, parsed.preventEmptyRollover)
        }
    }

    fun `test stream round-trip below target version preserves flat and drops groups`() {
        repeat(iterations) {
            val action = randomFlatAction()
            val version = randomBelowTargetVersion()
            val parsed = roundTripStream(action, version)
            assertEquals("minSize must round-trip below target", action.minSize, parsed.minSize)
            assertEquals("minDocs must round-trip below target", action.minDocs, parsed.minDocs)
            assertEquals("minAge must round-trip below target", action.minAge, parsed.minAge)
            assertEquals("minPrimaryShardSize must round-trip below target", action.minPrimaryShardSize, parsed.minPrimaryShardSize)
            assertTrue("Groups must be absent below target version", parsed.conditionGroups.isNullOrEmpty())
        }
    }

    fun `test pre-existing flat policy loads with no groups`() {
        repeat(iterations) {
            val action = randomFlatAction()
            val parsedXContent = parseFromXContent(action)
            assertTrue("Flat XContent must produce no groups", parsedXContent.conditionGroups.isNullOrEmpty())

            val parsedStream = roundTripStream(action, Version.V_3_3_0)
            assertTrue("Sub-target stream must produce no groups", parsedStream.conditionGroups.isNullOrEmpty())
        }
    }
}
