/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.io.stream.InputStreamStreamInput
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.opensearchapi.string
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
}
