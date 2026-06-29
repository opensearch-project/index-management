/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.filter
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.long
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
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

    private val groupArb: Arb<RolloverConditionGroup> = arbitrary {
        while (true) {
            val minSize = if (Arb.boolean().bind()) ByteSizeValue(Arb.long(1L..1_000_000L).bind()) else null
            val minDocs = if (Arb.boolean().bind()) Arb.long(1L..1_000_000L).bind() else null
            val minAge = if (Arb.boolean().bind()) TimeValue.timeValueMillis(Arb.long(1L..1_000_000L).bind()) else null
            val minPrimaryShardSize = if (Arb.boolean().bind()) ByteSizeValue(Arb.long(1L..1_000_000L).bind()) else null
            if (listOfNotNull(minSize, minDocs, minAge, minPrimaryShardSize).isNotEmpty()) {
                return@arbitrary RolloverConditionGroup(minSize, minDocs, minAge, minPrimaryShardSize)
            }
        }
        @Suppress("UNREACHABLE_CODE")
        error("unreachable")
    }

    private val groupedActionArb: Arb<RolloverAction> = arbitrary {
        RolloverAction(
            minSize = null, minDocs = null, minAge = null, minPrimaryShardSize = null,
            copyAlias = Arb.boolean().bind(),
            preventEmptyRollover = Arb.boolean().bind(),
            conditionGroups = Arb.list(groupArb, 1..4).bind(),
            index = 0,
        )
    }

    private val flatActionArb: Arb<RolloverAction> = arbitrary {
        while (true) {
            val minSize = if (Arb.boolean().bind()) ByteSizeValue(Arb.long(1L..1_000_000L).bind()) else null
            val minDocs = if (Arb.boolean().bind()) Arb.long(1L..1_000_000L).bind() else null
            val minAge = if (Arb.boolean().bind()) TimeValue.timeValueMillis(Arb.long(1L..1_000_000L).bind()) else null
            val minPrimaryShardSize = if (Arb.boolean().bind()) ByteSizeValue(Arb.long(1L..1_000_000L).bind()) else null
            if (listOfNotNull(minSize, minDocs, minAge, minPrimaryShardSize).isNotEmpty()) {
                return@arbitrary RolloverAction(
                    minSize = minSize, minDocs = minDocs, minAge = minAge, minPrimaryShardSize = minPrimaryShardSize,
                    copyAlias = Arb.boolean().bind(),
                    preventEmptyRollover = Arb.boolean().bind(),
                    index = 0,
                )
            }
        }
        @Suppress("UNREACHABLE_CODE")
        error("unreachable")
    }

    private val anyActionArb: Arb<RolloverAction> = arbitrary {
        if (Arb.boolean().bind()) groupedActionArb.bind() else flatActionArb.bind()
    }

    private val atOrAboveTargetVersionArb: Arb<Version> = Arb.element(Version.V_3_7_0, Version.CURRENT)

    private val belowTargetVersionArb: Arb<Version> =
        Arb.element(Version.V_3_3_0, Version.V_3_4_0, Version.V_3_5_0, Version.V_3_6_0)
            .filter { it.before(Version.V_3_7_0) }

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

    fun `test XContent round-trip preserves form and conditions`() = runBlocking {
        checkAll(100, anyActionArb) { action ->
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

    fun `test stream round-trip at or above target version preserves groups`() = runBlocking {
        checkAll(100, groupedActionArb, atOrAboveTargetVersionArb) { action, version ->
            val parsed = roundTripStream(action, version)
            assertTrue("Groups must round-trip at/above target version", groupsEqual(action.conditionGroups, parsed.conditionGroups))
            assertEquals("copyAlias must round-trip", action.copyAlias, parsed.copyAlias)
            assertEquals("preventEmptyRollover must round-trip", action.preventEmptyRollover, parsed.preventEmptyRollover)
        }
    }

    fun `test stream round-trip below target version preserves flat and drops groups`() = runBlocking {
        checkAll(100, flatActionArb, belowTargetVersionArb) { action, version ->
            val parsed = roundTripStream(action, version)
            assertEquals("minSize must round-trip below target", action.minSize, parsed.minSize)
            assertEquals("minDocs must round-trip below target", action.minDocs, parsed.minDocs)
            assertEquals("minAge must round-trip below target", action.minAge, parsed.minAge)
            assertEquals("minPrimaryShardSize must round-trip below target", action.minPrimaryShardSize, parsed.minPrimaryShardSize)
            assertTrue("Groups must be absent below target version", parsed.conditionGroups.isNullOrEmpty())
        }
    }

    fun `test pre-existing flat policy loads with no groups`() = runBlocking {
        checkAll(100, flatActionArb) { action ->
            val parsedXContent = parseFromXContent(action)
            assertTrue("Flat XContent must produce no groups", parsedXContent.conditionGroups.isNullOrEmpty())

            val parsedStream = roundTripStream(action, Version.V_3_3_0)
            assertTrue("Sub-target stream must produce no groups", parsedStream.conditionGroups.isNullOrEmpty())
        }
    }
}
