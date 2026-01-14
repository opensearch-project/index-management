/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.io.stream.InputStreamStreamInput
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.test.OpenSearchTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class ConvertIndexToRemoteActionTests : OpenSearchTestCase() {

    fun `test action with default rename_pattern`() {
        val action = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            index = 0,
        )

        assertEquals("renamePattern should default to \$1_remote", "\$1_remote", action.renamePattern)
    }

    fun `test action with custom rename_pattern`() {
        val action = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            renamePattern = "remote_\$1",
            index = 0,
        )

        assertEquals("renamePattern should be remote_\$1", "remote_\$1", action.renamePattern)
    }

    fun `test XContent serialization with default rename_pattern`() {
        val action = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            index = 0,
        )

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        action.populateAction(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS)
        builder.endObject()

        val jsonString = builder.string()
        assertTrue("XContent should contain repository", jsonString.contains("\"repository\":\"my-repo\""))
        assertTrue("XContent should contain snapshot", jsonString.contains("\"snapshot\":\"{{ctx.index}}\""))
        assertFalse("XContent should NOT contain rename_pattern when default", jsonString.contains("rename_pattern"))
    }

    fun `test XContent serialization with custom rename_pattern`() {
        val action = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            renamePattern = "remote_\$1",
            index = 0,
        )

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        action.populateAction(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS)
        builder.endObject()

        val jsonString = builder.string()
        assertTrue("XContent should contain rename_pattern", jsonString.contains("\"rename_pattern\":\"remote_\$1\""))
    }

    fun `test StreamOutput serialization round trip`() {
        val originalAction = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            renamePattern = "remote_\$1",
            index = 0,
        )

        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        originalAction.writeTo(osso)

        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        val deserializedAction = ISMActionsParser.instance.fromStreamInput(input) as ConvertIndexToRemoteAction

        assertEquals("repository should be preserved", originalAction.repository, deserializedAction.repository)
        assertEquals("snapshot should be preserved", originalAction.snapshot, deserializedAction.snapshot)
        assertEquals("renamePattern should be preserved", originalAction.renamePattern, deserializedAction.renamePattern)
    }

    fun `test XContent round trip with custom rename_pattern`() {
        val originalAction = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            renamePattern = "remote_\$1",
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
        parser.nextToken()

        val parsedAction = ISMActionsParser.instance.parse(parser, 0) as ConvertIndexToRemoteAction

        assertEquals("repository should match", originalAction.repository, parsedAction.repository)
        assertEquals("snapshot should match", originalAction.snapshot, parsedAction.snapshot)
        assertEquals("renamePattern should match", originalAction.renamePattern, parsedAction.renamePattern)
    }

    fun `test parsing without optional rename_pattern field uses default value`() {
        val jsonString = """
            {
                "convert_index_to_remote": {
                    "repository": "my-repo",
                    "snapshot": "{{ctx.index}}"
                }
            }
        """.trimIndent()

        val parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            jsonString,
        )
        parser.nextToken()

        val action = ISMActionsParser.instance.parse(parser, 0) as ConvertIndexToRemoteAction

        assertEquals("repository should be parsed", "my-repo", action.repository)
        assertEquals("snapshot should be parsed", "{{ctx.index}}", action.snapshot)
        assertEquals("renamePattern should default to \$1_remote", "\$1_remote", action.renamePattern)
    }

    fun `test parsing with rename_pattern field`() {
        val jsonString = """
            {
                "convert_index_to_remote": {
                    "repository": "my-repo",
                    "snapshot": "{{ctx.index}}",
                    "rename_pattern": "remote_$1"
                }
            }
        """.trimIndent()

        val parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            jsonString,
        )
        parser.nextToken()

        val action = ISMActionsParser.instance.parse(parser, 0) as ConvertIndexToRemoteAction

        assertEquals("repository should be parsed", "my-repo", action.repository)
        assertEquals("snapshot should be parsed", "{{ctx.index}}", action.snapshot)
        assertEquals("renamePattern should be parsed", "remote_\$1", action.renamePattern)
    }

    fun `test invalid field throws exception`() {
        val jsonString = """
            {
                "convert_index_to_remote": {
                    "repository": "my-repo",
                    "snapshot": "{{ctx.index}}",
                    "invalid_field": "value"
                }
            }
        """.trimIndent()

        val parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            jsonString,
        )
        parser.nextToken()

        assertThrows(IllegalArgumentException::class.java) {
            ISMActionsParser.instance.parse(parser, 0)
        }
    }

    fun `test missing repository throws exception`() {
        val jsonString = """
            {
                "convert_index_to_remote": {
                    "snapshot": "{{ctx.index}}"
                }
            }
        """.trimIndent()

        val parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            jsonString,
        )
        parser.nextToken()

        assertThrows(IllegalArgumentException::class.java) {
            ISMActionsParser.instance.parse(parser, 0)
        }
    }

    fun `test missing snapshot throws exception`() {
        val jsonString = """
            {
                "convert_index_to_remote": {
                    "repository": "my-repo"
                }
            }
        """.trimIndent()

        val parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            jsonString,
        )
        parser.nextToken()

        assertThrows(IllegalArgumentException::class.java) {
            ISMActionsParser.instance.parse(parser, 0)
        }
    }
}
