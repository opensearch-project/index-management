/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.indexstatemanagement.extension.SampleCustomActionParser
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

class GetPoliciesResponseTests : OpenSearchTestCase() {

    fun `test get policies response`() {
        val policy = randomPolicy()
        val res = GetPoliciesResponse(listOf(policy), 1)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = GetPoliciesResponse(sin)
        assertEquals(1, newRes.totalPolicies)
        assertEquals(1, newRes.policies.size)
        assertEquals(policy, newRes.policies[0])
    }

    @Suppress("UNCHECKED_CAST")
    fun `test get policies response custom action`() {
        val customActionParser = SampleCustomActionParser()
        customActionParser.customAction = true
        val extensionName = "testExtension"
        ISMActionsParser.instance.addParser(customActionParser, extensionName)
        val policyID = "policyID"
        val action = SampleCustomActionParser.SampleCustomAction(someInt = randomInt(), index = 0)
        val states = listOf(State(name = "CustomState", actions = listOf(action), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        val res = GetPoliciesResponse(listOf(policy), 1)

        val responseString = res.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()
        val responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, responseString, false)
        assertEquals("Round tripping custom action doesn't work", res.convertToMap(), responseMap)
        assertNotEquals("Get policies response should change the policy output", responseMap, policy.convertToMap())
        val parsedPolicy = (responseMap["policies"] as ArrayList<Map<String, Any>>).first()["policy"] as Map<String, Any>
        val parsedStates = parsedPolicy["states"] as ArrayList<Map<String, Any>>
        val parsedActions = parsedStates.first()["actions"] as ArrayList<Map<String, Any>>
        assertFalse("Get policies response should not contain the custom keyword", parsedActions.first().containsKey("custom"))
        ISMActionsParser.instance.parsers.removeIf { it.getActionType() == SampleCustomActionParser.SampleCustomAction.name }
    }
}
