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
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout
import org.opensearch.test.OpenSearchTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class RolloverActionTests : OpenSearchTestCase() {

    fun `test XContent serialization with preventEmptyRollover true`() {
        val action = RolloverAction(
            minSize = ByteSizeValue.parseBytesSizeValue("50gb", "test"),
            minDocs = 1000L,
            minAge = TimeValue.parseTimeValue("7d", "test"),
            minPrimaryShardSize = ByteSizeValue.parseBytesSizeValue("30gb", "test"),
            copyAlias = false,
            preventEmptyRollover = true,
            index = 0,
        )

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        action.populateAction(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS)
        builder.endObject()

        val jsonString = builder.string()
        assertTrue("XContent should contain prevent_empty_rollover field", jsonString.contains("\"prevent_empty_rollover\":true"))
    }

    fun `test XContent serialization with preventEmptyRollover false`() {
        val action = RolloverAction(
            minSize = ByteSizeValue.parseBytesSizeValue("50gb", "test"),
            minDocs = 1000L,
            minAge = TimeValue.parseTimeValue("7d", "test"),
            minPrimaryShardSize = ByteSizeValue.parseBytesSizeValue("30gb", "test"),
            copyAlias = false,
            preventEmptyRollover = false,
            index = 0,
        )

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        action.populateAction(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS)
        builder.endObject()

        val jsonString = builder.string()
        assertFalse(
            "XContent should NOT contain prevent_empty_rollover field when false",
            jsonString.contains("prevent_empty_rollover"),
        )
    }

    fun `test StreamOutput serialization`() {
        val originalAction = RolloverAction(
            minSize = ByteSizeValue.parseBytesSizeValue("50gb", "test"),
            minDocs = 1000L,
            minAge = TimeValue.parseTimeValue("7d", "test"),
            minPrimaryShardSize = ByteSizeValue.parseBytesSizeValue("30gb", "test"),
            copyAlias = true,
            preventEmptyRollover = true,
            index = 0,
        )

        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        originalAction.writeTo(osso)

        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        val deserializedAction = ISMActionsParser.instance.fromStreamInput(input) as RolloverAction

        assertEquals("minSize should be preserved", originalAction.minSize, deserializedAction.minSize)
        assertEquals("minDocs should be preserved", originalAction.minDocs, deserializedAction.minDocs)
        assertEquals("minAge should be preserved", originalAction.minAge, deserializedAction.minAge)
        assertEquals("minPrimaryShardSize should be preserved", originalAction.minPrimaryShardSize, deserializedAction.minPrimaryShardSize)
        assertEquals("copyAlias should be preserved", originalAction.copyAlias, deserializedAction.copyAlias)
        assertEquals("preventEmptyRollover should be preserved", originalAction.preventEmptyRollover, deserializedAction.preventEmptyRollover)
    }

    fun `test backward compatibility deserialization`() {
        // Create XContent without prevent_empty_rollover field
        val jsonString = """
            {
                "rollover": {
                    "min_size": "50gb",
                    "min_doc_count": 1000,
                    "min_index_age": "7d",
                    "min_primary_shard_size": "30gb",
                    "copy_alias": false
                }
            }
        """.trimIndent()

        val parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            jsonString,
        )
        parser.nextToken()

        val action = ISMActionsParser.instance.parse(parser, 0) as RolloverAction

        assertFalse("preventEmptyRollover should default to false when not present", action.preventEmptyRollover)
        assertEquals("minSize should be parsed correctly", ByteSizeValue.parseBytesSizeValue("50gb", "test"), action.minSize)
        assertEquals("minDocs should be parsed correctly", 1000L, action.minDocs)
        assertFalse("copyAlias should be parsed correctly", action.copyAlias)
    }

    fun `test preventEmptyRollover field defaults to false`() {
        val action = RolloverAction(
            minSize = ByteSizeValue.parseBytesSizeValue("50gb", "test"),
            minDocs = null,
            minAge = null,
            minPrimaryShardSize = null,
            copyAlias = false,
            preventEmptyRollover = false,
            index = 0,
        )

        assertFalse("preventEmptyRollover should default to false", action.preventEmptyRollover)
    }

    fun `test preventEmptyRollover can be set to true`() {
        val action = RolloverAction(
            minSize = null,
            minDocs = null,
            minAge = TimeValue.parseTimeValue("7d", "test"),
            minPrimaryShardSize = null,
            copyAlias = false,
            preventEmptyRollover = true,
            index = 0,
        )

        assertTrue("preventEmptyRollover should be true", action.preventEmptyRollover)
    }

    fun `test preventEmptyRollover with all conditions`() {
        val action = RolloverAction(
            minSize = ByteSizeValue.parseBytesSizeValue("50gb", "test"),
            minDocs = 1000L,
            minAge = TimeValue.parseTimeValue("7d", "test"),
            minPrimaryShardSize = ByteSizeValue.parseBytesSizeValue("30gb", "test"),
            copyAlias = true,
            preventEmptyRollover = true,
            index = 0,
        )

        assertTrue("preventEmptyRollover should be true", action.preventEmptyRollover)
        assertTrue("copyAlias should be true", action.copyAlias)
        assertEquals("minSize should be preserved", ByteSizeValue.parseBytesSizeValue("50gb", "test"), action.minSize)
        assertEquals("minDocs should be preserved", 1000L, action.minDocs)
        assertEquals("minAge should be preserved", TimeValue.parseTimeValue("7d", "test"), action.minAge)
        assertEquals(
            "minPrimaryShardSize should be preserved",
            ByteSizeValue.parseBytesSizeValue("30gb", "test"), action.minPrimaryShardSize,
        )
    }

    fun `test XContent round trip with all fields`() {
        val originalAction = RolloverAction(
            minSize = ByteSizeValue.parseBytesSizeValue("50gb", "test"),
            minDocs = 1000L,
            minAge = TimeValue.parseTimeValue("7d", "test"),
            minPrimaryShardSize = ByteSizeValue.parseBytesSizeValue("30gb", "test"),
            copyAlias = true,
            preventEmptyRollover = true,
            index = 0,
        )

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        originalAction.populateAction(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS)
        builder.endObject()

        val parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            builder.string(),
        )
        parser.nextToken() // Move to START_OBJECT

        val parsedAction = ISMActionsParser.instance.parse(parser, 0) as RolloverAction

        assertEquals("minSize should match", originalAction.minSize, parsedAction.minSize)
        assertEquals("minDocs should match", originalAction.minDocs, parsedAction.minDocs)
        assertEquals("minAge should match", originalAction.minAge, parsedAction.minAge)
        assertEquals("minPrimaryShardSize should match", originalAction.minPrimaryShardSize, parsedAction.minPrimaryShardSize)
        assertEquals("copyAlias should match", originalAction.copyAlias, parsedAction.copyAlias)
        assertEquals("preventEmptyRollover should match", originalAction.preventEmptyRollover, parsedAction.preventEmptyRollover)
    }

    fun `test preventEmptyRollover with only minAge condition`() {
        val action = RolloverAction(
            minSize = null,
            minDocs = null,
            minAge = TimeValue.parseTimeValue("1d", "test"),
            minPrimaryShardSize = null,
            copyAlias = false,
            preventEmptyRollover = true,
            index = 0,
        )

        assertTrue("preventEmptyRollover should be true", action.preventEmptyRollover)
        assertNull("minSize should be null", action.minSize)
        assertNull("minDocs should be null", action.minDocs)
        assertNotNull("minAge should not be null", action.minAge)
        assertNull("minPrimaryShardSize should be null", action.minPrimaryShardSize)
    }

    fun `test preventEmptyRollover with only minDocs condition`() {
        val action = RolloverAction(
            minSize = null,
            minDocs = 100L,
            minAge = null,
            minPrimaryShardSize = null,
            copyAlias = false,
            preventEmptyRollover = true,
            index = 0,
        )

        assertTrue("preventEmptyRollover should be true", action.preventEmptyRollover)
        assertNull("minSize should be null", action.minSize)
        assertEquals("minDocs should be 100", 100L, action.minDocs)
        assertNull("minAge should be null", action.minAge)
        assertNull("minPrimaryShardSize should be null", action.minPrimaryShardSize)
    }

    fun `test preventEmptyRollover with copyAlias true`() {
        val action = RolloverAction(
            minSize = null,
            minDocs = null,
            minAge = TimeValue.parseTimeValue("1d", "test"),
            minPrimaryShardSize = null,
            copyAlias = true,
            preventEmptyRollover = true,
            index = 0,
        )

        assertTrue("preventEmptyRollover should be true", action.preventEmptyRollover)
        assertTrue("copyAlias should be true", action.copyAlias)
    }

    fun `test StreamOutput round trip with mixed conditions`() {
        val originalAction = RolloverAction(
            minSize = ByteSizeValue.parseBytesSizeValue("10gb", "test"),
            minDocs = null,
            minAge = TimeValue.parseTimeValue("3d", "test"),
            minPrimaryShardSize = null,
            copyAlias = false,
            preventEmptyRollover = true,
            index = 0,
        )

        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        originalAction.writeTo(osso)

        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        val deserializedAction = ISMActionsParser.instance.fromStreamInput(input) as RolloverAction

        assertEquals("minSize should be preserved", originalAction.minSize, deserializedAction.minSize)
        assertNull("minDocs should be null", deserializedAction.minDocs)
        assertEquals("minAge should be preserved", originalAction.minAge, deserializedAction.minAge)
        assertNull("minPrimaryShardSize should be null", deserializedAction.minPrimaryShardSize)
        assertEquals("copyAlias should be preserved", originalAction.copyAlias, deserializedAction.copyAlias)
        assertEquals("preventEmptyRollover should be preserved", originalAction.preventEmptyRollover, deserializedAction.preventEmptyRollover)
    }

    fun `test backward compatibility - old version without preventEmptyRollover`() {
        // Simulate serialization from an older version (< 3.4.0) that doesn't have preventEmptyRollover
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        osso.version = Version.V_3_3_0

        // Write action type and config fields as ISMActionsParser expects
        osso.writeString("rollover")
        osso.writeOptionalWriteable(null) // configTimeout
        osso.writeOptionalWriteable(null) // configRetry

        // Manually write fields as an old version would (without preventEmptyRollover)
        osso.writeOptionalWriteable(ByteSizeValue.parseBytesSizeValue("50gb", "test"))
        osso.writeOptionalLong(1000L)
        osso.writeOptionalTimeValue(TimeValue.parseTimeValue("7d", "test"))
        osso.writeOptionalWriteable(ByteSizeValue.parseBytesSizeValue("30gb", "test"))
        osso.writeBoolean(true) // copyAlias
        // Note: preventEmptyRollover is NOT written for old version
        osso.writeInt(0) // actionIndex

        // Deserialize with current version parser
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        input.version = Version.V_3_3_0
        val deserializedAction = ISMActionsParser.instance.fromStreamInput(input) as RolloverAction

        assertEquals("minSize should be preserved", ByteSizeValue.parseBytesSizeValue("50gb", "test"), deserializedAction.minSize)
        assertEquals("minDocs should be preserved", 1000L, deserializedAction.minDocs)
        assertEquals("minAge should be preserved", TimeValue.parseTimeValue("7d", "test"), deserializedAction.minAge)
        assertEquals("minPrimaryShardSize should be preserved", ByteSizeValue.parseBytesSizeValue("30gb", "test"), deserializedAction.minPrimaryShardSize)
        assertTrue("copyAlias should be true", deserializedAction.copyAlias)
        assertFalse("preventEmptyRollover should default to false for old version", deserializedAction.preventEmptyRollover)
    }

    fun `test forward compatibility - new version with preventEmptyRollover`() {
        // Test that new version (>= 3.4.0) correctly serializes and deserializes preventEmptyRollover
        val originalAction = RolloverAction(
            minSize = ByteSizeValue.parseBytesSizeValue("50gb", "test"),
            minDocs = 1000L,
            minAge = TimeValue.parseTimeValue("7d", "test"),
            minPrimaryShardSize = ByteSizeValue.parseBytesSizeValue("30gb", "test"),
            copyAlias = true,
            preventEmptyRollover = true,
            index = 0,
        )

        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        osso.version = Version.V_3_4_0
        originalAction.writeTo(osso)

        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        input.version = Version.V_3_4_0
        val deserializedAction = ISMActionsParser.instance.fromStreamInput(input) as RolloverAction

        assertEquals("minSize should be preserved", originalAction.minSize, deserializedAction.minSize)
        assertEquals("minDocs should be preserved", originalAction.minDocs, deserializedAction.minDocs)
        assertEquals("minAge should be preserved", originalAction.minAge, deserializedAction.minAge)
        assertEquals("minPrimaryShardSize should be preserved", originalAction.minPrimaryShardSize, deserializedAction.minPrimaryShardSize)
        assertEquals("copyAlias should be preserved", originalAction.copyAlias, deserializedAction.copyAlias)
        assertTrue("preventEmptyRollover should be preserved as true", deserializedAction.preventEmptyRollover)
    }

    fun `test version compatibility - writing to old version skips preventEmptyRollover`() {
        // Test that when writing to an old version node, preventEmptyRollover is not serialized
        val action = RolloverAction(
            minSize = ByteSizeValue.parseBytesSizeValue("50gb", "test"),
            minDocs = 1000L,
            minAge = TimeValue.parseTimeValue("7d", "test"),
            minPrimaryShardSize = ByteSizeValue.parseBytesSizeValue("30gb", "test"),
            copyAlias = true,
            preventEmptyRollover = true,
            index = 0,
        )

        // Serialize with old version (< 3.4.0)
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        osso.version = Version.V_3_3_0

        // Write action type and config fields as ISMActionsParser expects
        osso.writeString("rollover")
        osso.writeOptionalWriteable(action.configTimeout)
        osso.writeOptionalWriteable(action.configRetry)

        // Now write the action itself
        action.populateAction(osso)

        // The byte array should NOT contain the preventEmptyRollover field
        // We can verify this by deserializing with old version expectations
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        input.version = Version.V_3_3_0

        // Skip the action type and config fields that ISMActionsParser would read
        input.readString() // action type
        input.readOptionalWriteable(::ActionTimeout) // configTimeout
        input.readOptionalWriteable(::ActionRetry) // configRetry

        // Read fields as old version would
        val minSize = input.readOptionalWriteable(::ByteSizeValue)
        val minDocs = input.readOptionalLong()
        val minAge = input.readOptionalTimeValue()
        val minPrimaryShardSize = input.readOptionalWriteable(::ByteSizeValue)
        val copyAlias = input.readBoolean()
        // Old version doesn't read preventEmptyRollover
        val actionIndex = input.readInt()

        assertEquals("minSize should match", action.minSize, minSize)
        assertEquals("minDocs should match", action.minDocs, minDocs)
        assertEquals("minAge should match", action.minAge, minAge)
        assertEquals("minPrimaryShardSize should match", action.minPrimaryShardSize, minPrimaryShardSize)
        assertEquals("copyAlias should match", action.copyAlias, copyAlias)
        assertEquals("actionIndex should match", action.actionIndex, actionIndex)
        assertEquals("Stream should be fully consumed", 0, input.available())
    }

    fun `test parser flat default produces no condition groups`() {
        val jsonString = """
            {
                "rollover": {
                    "min_size": "50gb",
                    "min_doc_count": 1000
                }
            }
        """.trimIndent()

        val parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(), LoggingDeprecationHandler.INSTANCE, jsonString,
        )
        parser.nextToken()
        val action = ISMActionsParser.instance.parse(parser, 0) as RolloverAction

        assertNull("Flat policy should have no condition groups", action.conditionGroups)
        assertEquals("minSize should be parsed", ByteSizeValue.parseBytesSizeValue("50gb", "test"), action.minSize)
        assertEquals("minDocs should be parsed", 1000L, action.minDocs)
    }

    fun `test parser grouped any_of produces expected groups`() {
        val jsonString = """
            {
                "rollover": {
                    "any_of": [
                        { "min_index_age": "7d", "min_size": "50gb" },
                        { "min_doc_count": 100000000 }
                    ]
                }
            }
        """.trimIndent()

        val parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(), LoggingDeprecationHandler.INSTANCE, jsonString,
        )
        parser.nextToken()
        val action = ISMActionsParser.instance.parse(parser, 0) as RolloverAction

        val groups = action.conditionGroups
        assertNotNull("Grouped policy should have condition groups", groups)
        assertEquals("Should have two groups", 2, groups!!.size)

        val first = groups[0]
        assertEquals("First group minAge", TimeValue.parseTimeValue("7d", "test"), first.minAge)
        assertEquals("First group minSize", ByteSizeValue.parseBytesSizeValue("50gb", "test"), first.minSize)
        assertNull("First group minDocs should be null", first.minDocs)

        val second = groups[1]
        assertEquals("Second group minDocs", 100000000L, second.minDocs)
        assertNull("Second group minAge should be null", second.minAge)

        assertNull("Flat minSize should be null", action.minSize)
        assertNull("Flat minDocs should be null", action.minDocs)
    }

    fun `test mixed-version serialization at target version preserves groups`() {
        val originalAction = RolloverAction(
            minSize = null, minDocs = null, minAge = null, minPrimaryShardSize = null,
            copyAlias = true,
            preventEmptyRollover = true,
            conditionGroups = listOf(
                RolloverConditionGroup(
                    minSize = ByteSizeValue.parseBytesSizeValue("50gb", "test"),
                    minDocs = null,
                    minAge = TimeValue.parseTimeValue("7d", "test"),
                    minPrimaryShardSize = null,
                ),
                RolloverConditionGroup(minSize = null, minDocs = 100L, minAge = null, minPrimaryShardSize = null),
            ),
            index = 0,
        )

        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        osso.version = RolloverAction.TARGET_VERSION
        originalAction.writeTo(osso)

        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        input.version = RolloverAction.TARGET_VERSION
        val deserialized = ISMActionsParser.instance.fromStreamInput(input) as RolloverAction

        assertEquals("Groups should be preserved at target version", originalAction.conditionGroups, deserialized.conditionGroups)
        assertEquals("copyAlias should be preserved", originalAction.copyAlias, deserialized.copyAlias)
        assertEquals("preventEmptyRollover should be preserved", originalAction.preventEmptyRollover, deserialized.preventEmptyRollover)
    }

    fun `test mixed-version serialization below target version preserves flat and drops groups`() {
        val flatAction = RolloverAction(
            minSize = ByteSizeValue.parseBytesSizeValue("10gb", "test"),
            minDocs = null,
            minAge = TimeValue.parseTimeValue("3d", "test"),
            minPrimaryShardSize = null,
            copyAlias = false,
            preventEmptyRollover = false,
            index = 0,
        )

        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        osso.version = Version.V_3_3_0
        flatAction.writeTo(osso)

        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        input.version = Version.V_3_3_0
        val deserialized = ISMActionsParser.instance.fromStreamInput(input) as RolloverAction

        assertEquals("minSize should be preserved below target", flatAction.minSize, deserialized.minSize)
        assertEquals("minAge should be preserved below target", flatAction.minAge, deserialized.minAge)
        assertNull("Groups must be absent below target version", deserialized.conditionGroups)
        assertEquals("Stream should be fully consumed", 0, input.available())
    }
}
