/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.extension

import org.junit.After
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class ISMActionsParserTests : OpenSearchTestCase() {

    val extensionName = "testExtension"

    /*
     * If any tests added the custom action parser, it should be removed from the static instance to not impact other tests
     */
    @After
    @Suppress("UnusedPrivateMember")
    private fun removeCustomActionParser() {
        ISMActionsParser.instance.parsers.removeIf { it.getActionType() == SampleCustomActionParser.SampleCustomAction.name }
    }

    fun `test duplicate action names fail`() {
        val customActionParser = SampleCustomActionParser()
        // Duplicate custom parser names should fail
        ISMActionsParser.instance.addParser(customActionParser, extensionName)
        assertFailsWith<IllegalArgumentException>("Expected IllegalArgumentException for duplicate action names") {
            ISMActionsParser.instance.addParser(customActionParser, extensionName)
        }
        // Adding any duplicate parser should fail
        assertFailsWith<IllegalArgumentException>("Expected IllegalArgumentException for duplicate action names") {
            val randomExistingParser = ISMActionsParser.instance.parsers.random()
            ISMActionsParser.instance.addParser(randomExistingParser, extensionName)
        }
    }

    fun `test custom action parsing`() {
        val customActionParser = SampleCustomActionParser()
        ISMActionsParser.instance.addParser(customActionParser, extensionName)
        val customAction = SampleCustomActionParser.SampleCustomAction(randomInt(), 0)
        val builder = XContentFactory.jsonBuilder()

        val customActionString = customAction.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, customActionString)
        parser.nextToken()
        val parsedCustomAction = ISMActionsParser.instance.parse(parser, 1)
        assertTrue("Action was not set to be custom after parsing", parsedCustomAction.customAction)
        customAction.customAction = true
        assertEquals("Round tripping custom action doesn't work", customAction.convertToMap(), parsedCustomAction.convertToMap())
    }

    fun `test parsing custom action without custom flag`() {
        val customActionParser = SampleCustomActionParser()
        ISMActionsParser.instance.addParser(customActionParser, extensionName)
        val customAction = SampleCustomActionParser.SampleCustomAction(randomInt(), 0)
        customAction.customAction = true

        val customActionString = "{\"retry\":{\"count\":3,\"backoff\":\"exponential\",\"delay\":\"1m\"},\"some_custom_action\":{\"some_int_field\":${customAction.someInt}}}"
        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, customActionString)
        parser.nextToken()
        val parsedCustomAction = ISMActionsParser.instance.parse(parser, 1)
        assertTrue("Action was not set to be custom after parsing", parsedCustomAction.customAction)
        assertEquals("Round tripping custom action doesn't work", customAction.convertToMap(), parsedCustomAction.convertToMap())
        assertTrue("Custom action did not have custom keyword after parsing", parsedCustomAction.convertToMap().containsKey("custom"))
    }

    fun `test parsing custom action with custom flag`() {
        val customActionParser = SampleCustomActionParser()
        ISMActionsParser.instance.addParser(customActionParser, extensionName)
        val customAction = SampleCustomActionParser.SampleCustomAction(randomInt(), 0)
        customAction.customAction = true

        val customActionString = "{\"retry\":{\"count\":3,\"backoff\":\"exponential\",\"delay\":\"1m\"},\"custom\": {\"some_custom_action\":{\"some_int_field\":${customAction.someInt}}}}"
        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, customActionString)
        parser.nextToken()
        val parsedCustomAction = ISMActionsParser.instance.parse(parser, 1)
        assertTrue("Action was not set to be custom after parsing", parsedCustomAction.customAction)
        assertEquals("Round tripping custom action doesn't work", customAction.convertToMap(), parsedCustomAction.convertToMap())
        assertTrue("Custom action did not have custom keyword after parsing", parsedCustomAction.convertToMap().containsKey("custom"))
    }
}
