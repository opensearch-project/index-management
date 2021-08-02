/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ReadOnlyActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ISMTemplateRestAPIIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    private val policyID1 = "t1"
    private val policyID2 = "t2"
    private val policyID3 = "t3"

    fun `test add template with invalid index pattern`() {
        try {
            val ismTemp = ISMTemplate(listOf(" "), 100, randomInstant())
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp)), policyID1)
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()["error"] as Map<String, Any>
            val expectedReason = "Validation Failed: 1: index_pattern [ ] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
            assertEquals(expectedReason, actualMessage["reason"])
        }
    }

    fun `test add template with self-overlapping index pattern`() {
        try {
            val ismTemp = ISMTemplate(listOf("ab*"), 100, randomInstant())
            val ismTemp2 = ISMTemplate(listOf("abc*"), 100, randomInstant())
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp, ismTemp2)), policyID1)
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()["error"] as Map<String, Any>
            val expectedReason = "New policy $policyID1 has an ISM template with index pattern [ab*] matching this policy's other ISM templates with index patterns [abc*], please use different priority"
            assertEquals(expectedReason, actualMessage["reason"])
        }
    }

    fun `test add template with overlapping index pattern`() {
        try {
            val ismTemp = ISMTemplate(listOf("log*"), 100, randomInstant())
            val ismTemp2 = ISMTemplate(listOf("abc*"), 100, randomInstant())
            val ismTemp3 = ISMTemplate(listOf("*"), 100, randomInstant())
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp)), policyID1)
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp2)), policyID2)
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp3)), policyID3)
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()["error"] as Map<String, Any>
            val expectedReason = "New policy $policyID3 has an ISM template with index pattern [*] matching existing policy templates, please use a different priority than 100"
            assertEquals(expectedReason, actualMessage["reason"])
        }
    }

    fun `test ism template managing index`() {
        val indexName1 = "log-000001"
        val indexName2 = "log-000002"
        val indexName3 = "log-000003"
        val policyID = "${testIndexName}_testPolicyName_1"

        // need to specify policyID null, can remove after policyID deprecated
        createIndex(indexName1, null)

        val ismTemp = ISMTemplate(listOf("log*"), 100, randomInstant())

        val actionConfig = ReadOnlyActionConfig(0)
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
            ismTemplate = listOf(ismTemp)
        )
        createPolicy(policy, policyID)

        createIndex(indexName2, null)
        createIndex(indexName3, Settings.builder().put(INDEX_HIDDEN, true).build())

        waitFor { assertNotNull(getManagedIndexConfig(indexName2)) }

        // TODO uncomment in remove policy id
        // val managedIndexConfig = getExistingManagedIndexConfig(indexName2)
        // updateManagedIndexConfigStartTime(managedIndexConfig)
        // waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName2).policyID) }

        // only index create after template can be managed
        assertPredicatesOnMetaData(
            listOf(
                indexName1 to listOf(
                    explainResponseOpendistroPolicyIdSetting to fun(policyID: Any?): Boolean = policyID == null,
                    explainResponseOpenSearchPolicyIdSetting to fun(policyID: Any?): Boolean = policyID == null
                )
            ),
            getExplainMap(indexName1),
            true
        )
        assertNull(getManagedIndexConfig(indexName1))

        // hidden index will not be manage
        assertPredicatesOnMetaData(
            listOf(
                indexName1 to listOf(
                    explainResponseOpendistroPolicyIdSetting to fun(policyID: Any?): Boolean = policyID == null,
                    explainResponseOpenSearchPolicyIdSetting to fun(policyID: Any?): Boolean = policyID == null
                )
            ),
            getExplainMap(indexName1),
            true
        )
        assertNull(getManagedIndexConfig(indexName3))
    }
}
