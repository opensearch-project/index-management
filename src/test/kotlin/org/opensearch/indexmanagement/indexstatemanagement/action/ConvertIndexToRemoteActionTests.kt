/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.Version
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.io.stream.InputStreamStreamInput
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.DEFAULT_RENAME_PATTERN
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout
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

    fun `test add_original_name_as_alias defaults to false`() {
        val action = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            index = 0,
        )

        assertEquals("addOriginalNameAsAlias should default to false", false, action.addOriginalNameAsAlias)
    }

    fun `test add_original_name_as_alias with delete_original_index true`() {
        val action = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            deleteOriginalIndex = true,
            addOriginalNameAsAlias = true,
            index = 0,
        )

        assertEquals("addOriginalNameAsAlias should be true", true, action.addOriginalNameAsAlias)
        assertEquals("deleteOriginalIndex should be true", true, action.deleteOriginalIndex)
    }

    fun `test add_original_name_as_alias without delete_original_index throws exception`() {
        assertThrows(IllegalArgumentException::class.java) {
            ConvertIndexToRemoteAction(
                repository = "my-repo",
                snapshot = "{{ctx.index}}",
                deleteOriginalIndex = false,
                addOriginalNameAsAlias = true,
                index = 0,
            )
        }
    }

    fun `test XContent serialization with add_original_name_as_alias`() {
        val action = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            deleteOriginalIndex = true,
            addOriginalNameAsAlias = true,
            index = 0,
        )

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        action.populateAction(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS)
        builder.endObject()

        val jsonString = builder.string()
        assertTrue("XContent should contain add_original_name_as_alias", jsonString.contains("\"add_original_name_as_alias\":true"))
        assertTrue("XContent should contain delete_original_index", jsonString.contains("\"delete_original_index\":true"))
    }

    fun `test XContent round trip with add_original_name_as_alias`() {
        val originalAction = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            includeAliases = true,
            deleteOriginalIndex = true,
            addOriginalNameAsAlias = true,
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
        assertEquals("includeAliases should match", originalAction.includeAliases, parsedAction.includeAliases)
        assertEquals("deleteOriginalIndex should match", originalAction.deleteOriginalIndex, parsedAction.deleteOriginalIndex)
        assertEquals("addOriginalNameAsAlias should match", originalAction.addOriginalNameAsAlias, parsedAction.addOriginalNameAsAlias)
        assertEquals("renamePattern should match", originalAction.renamePattern, parsedAction.renamePattern)
    }

    fun `test parsing with add_original_name_as_alias field`() {
        val jsonString = """
            {
                "convert_index_to_remote": {
                    "repository": "my-repo",
                    "snapshot": "{{ctx.index}}",
                    "delete_original_index": true,
                    "add_original_name_as_alias": true,
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
        assertEquals("deleteOriginalIndex should be true", true, action.deleteOriginalIndex)
        assertEquals("addOriginalNameAsAlias should be true", true, action.addOriginalNameAsAlias)
        assertEquals("renamePattern should be parsed", "remote_\$1", action.renamePattern)
    }

    fun `test parsing add_original_name_as_alias without delete_original_index throws`() {
        val jsonString = """
            {
                "convert_index_to_remote": {
                    "repository": "my-repo",
                    "snapshot": "{{ctx.index}}",
                    "add_original_name_as_alias": true
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

    fun `test StreamOutput round trip with add_original_name_as_alias`() {
        val originalAction = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            includeAliases = true,
            deleteOriginalIndex = true,
            addOriginalNameAsAlias = true,
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
        assertEquals("includeAliases should be preserved", originalAction.includeAliases, deserializedAction.includeAliases)
        assertEquals("deleteOriginalIndex should be preserved", originalAction.deleteOriginalIndex, deserializedAction.deleteOriginalIndex)
        assertEquals("addOriginalNameAsAlias should be preserved", originalAction.addOriginalNameAsAlias, deserializedAction.addOriginalNameAsAlias)
        assertEquals("renamePattern should be preserved", originalAction.renamePattern, deserializedAction.renamePattern)
    }

    fun `test backward compatibility - old version without new fields`() {
        // Stream written by pre-3.6.0 node: only repository, snapshot, index (no new fields, no renamePattern)
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        osso.version = Version.V_3_4_0

        osso.writeString("convert_index_to_remote")
        osso.writeOptionalWriteable(null) // configTimeout
        osso.writeOptionalWriteable(null) // configRetry
        osso.writeString("my-repo")
        osso.writeString("{{ctx.index}}")
        // V_3_4_0 < V_3_7_0: new fields and renamePattern are NOT written
        osso.writeInt(0) // actionIndex

        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        input.version = Version.V_3_4_0
        val deserializedAction = ISMActionsParser.instance.fromStreamInput(input) as ConvertIndexToRemoteAction

        assertEquals("repository should be preserved", "my-repo", deserializedAction.repository)
        assertEquals("snapshot should be preserved", "{{ctx.index}}", deserializedAction.snapshot)
        assertEquals("renamePattern should default", DEFAULT_RENAME_PATTERN, deserializedAction.renamePattern)
        assertEquals("includeAliases should default", false, deserializedAction.includeAliases)
        assertEquals("ignoreIndexSettings should default", "", deserializedAction.ignoreIndexSettings)
        assertNull("numberOfReplicas should default to null", deserializedAction.numberOfReplicas)
        assertEquals("deleteOriginalIndex should default", false, deserializedAction.deleteOriginalIndex)
        assertEquals("addOriginalNameAsAlias should default", false, deserializedAction.addOriginalNameAsAlias)
    }

    fun `test forward compatibility - new version with renamePattern`() {
        val originalAction = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            renamePattern = "remote_\$1",
            index = 0,
        )

        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        osso.version = Version.V_3_6_0
        originalAction.writeTo(osso)

        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        input.version = Version.V_3_6_0
        val deserializedAction = ISMActionsParser.instance.fromStreamInput(input) as ConvertIndexToRemoteAction

        assertEquals("repository should be preserved", originalAction.repository, deserializedAction.repository)
        assertEquals("snapshot should be preserved", originalAction.snapshot, deserializedAction.snapshot)
        assertEquals("renamePattern should be preserved", originalAction.renamePattern, deserializedAction.renamePattern)
    }

    fun `test version compatibility - writing to old version skips new fields and renamePattern`() {
        val action = ConvertIndexToRemoteAction(
            repository = "my-repo",
            snapshot = "{{ctx.index}}",
            renamePattern = "remote_\$1",
            index = 0,
        )

        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        osso.version = Version.V_3_4_0

        osso.writeString("convert_index_to_remote")
        osso.writeOptionalWriteable(action.configTimeout)
        osso.writeOptionalWriteable(action.configRetry)
        action.populateAction(osso)

        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))
        input.version = Version.V_3_4_0

        input.readString() // action type
        input.readOptionalWriteable(::ActionTimeout)
        input.readOptionalWriteable(::ActionRetry)

        // V_3_4_0 < V_3_7_0: new fields and renamePattern are NOT written, so we only read repository, snapshot, actionIndex
        val repository = input.readString()
        val snapshot = input.readString()
        val actionIndex = input.readInt()

        assertEquals("repository should match", action.repository, repository)
        assertEquals("snapshot should match", action.snapshot, snapshot)
        assertEquals("actionIndex should match", action.actionIndex, actionIndex)
        assertEquals("Stream should be fully consumed", 0, input.available())
    }
}
