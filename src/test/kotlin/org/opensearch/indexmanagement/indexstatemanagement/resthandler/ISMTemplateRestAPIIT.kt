/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadOnlyAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ISMTemplateRestAPIIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    private val policyID1 = "t1"
    private val policyID2 = "t2"
    private val policyID3 = "t3"

    @Suppress("UNCHECKED_CAST")
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

    @Suppress("UNCHECKED_CAST")
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

    @Suppress("UNCHECKED_CAST")
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

        val action = ReadOnlyAction(0)
        val states =
            listOf(
                State("ReadOnlyState", listOf(action), listOf()),
            )
        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
                ismTemplate = listOf(ismTemp),
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
                indexName1 to
                    listOf(
                        explainResponseOpendistroPolicyIdSetting to

                            fun(policyID: Any?): Boolean = policyID == null,
                        explainResponseOpenSearchPolicyIdSetting to

                            fun(policyID: Any?): Boolean = policyID == null,
                        ManagedIndexMetaData.ENABLED to

                            fun(enabled: Any?): Boolean = enabled == null,
                    ),
            ),
            getExplainMap(indexName1),
            true,
        )
        assertNull(getManagedIndexConfig(indexName1))

        // hidden index will not be manage
        assertPredicatesOnMetaData(
            listOf(
                indexName1 to
                    listOf(
                        explainResponseOpendistroPolicyIdSetting to

                            fun(policyID: Any?): Boolean = policyID == null,
                        explainResponseOpenSearchPolicyIdSetting to

                            fun(policyID: Any?): Boolean = policyID == null,
                        ManagedIndexMetaData.ENABLED to

                            fun(enabled: Any?): Boolean = enabled == null,
                    ),
            ),
            getExplainMap(indexName1),
            true,
        )
        assertNull(getManagedIndexConfig(indexName3))
    }

    fun `test ism template with exclusion patterns`() {
        val indexName1 = "log-production-001"
        val indexName2 = "log-test-001"
        val indexName3 = "log-staging-001"
        val indexName4 = "log-production-debug-001"
        val policyID = "${testIndexName}_testPolicyName_exclusion"

        // Create an ISM template with inclusion and exclusion patterns
        // Should match log-* but exclude log-test-* and log-*-debug-*
        val ismTemp = ISMTemplate(listOf("log-*", "-log-test-*", "-log-*-debug-*"), 100, randomInstant())

        val action = ReadOnlyAction(0)
        val states =
            listOf(
                State("ReadOnlyState", listOf(action), listOf()),
            )
        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description with exclusion patterns",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
                ismTemplate = listOf(ismTemp),
            )
        createPolicy(policy, policyID)

        // Create indices after policy
        createIndex(indexName1, null) // Should be managed (matches log-*, not excluded)
        createIndex(indexName2, null) // Should NOT be managed (matches log-test-*)
        createIndex(indexName3, null) // Should be managed (matches log-*, not excluded)
        createIndex(indexName4, null) // Should NOT be managed (matches log-*-debug-*)

        // Wait for coordinator to pick up and manage the matching indices
        waitFor { assertNotNull(getManagedIndexConfig(indexName1)) }
        waitFor { assertNotNull(getManagedIndexConfig(indexName3)) }

        // Verify log-production-001 IS managed with correct policy
        val managedConfig1 = getManagedIndexConfig(indexName1)
        assertNotNull("log-production-001 should be managed", managedConfig1)
        assertEquals("log-production-001 should be managed by the exclusion policy", policyID, managedConfig1!!.policyID)
        assertEquals("log-production-001 index name should match", indexName1, managedConfig1.index)

        // Verify log-staging-001 IS managed with correct policy
        val managedConfig3 = getManagedIndexConfig(indexName3)
        assertNotNull("log-staging-001 should be managed", managedConfig3)
        assertEquals("log-staging-001 should be managed by the exclusion policy", policyID, managedConfig3!!.policyID)
        assertEquals("log-staging-001 index name should match", indexName3, managedConfig3.index)

        // Verify managed indices have the policy applied in explain API
        assertPredicatesOnMetaData(
            listOf(
                indexName1 to
                    listOf(
                        explainResponseOpenSearchPolicyIdSetting to
                            fun(policyIDFromExplain: Any?): Boolean = policyIDFromExplain == policyID,
                    ),
            ),
            getExplainMap(indexName1),
            false,
        )

        assertPredicatesOnMetaData(
            listOf(
                indexName3 to
                    listOf(
                        explainResponseOpenSearchPolicyIdSetting to
                            fun(policyIDFromExplain: Any?): Boolean = policyIDFromExplain == policyID,
                    ),
            ),
            getExplainMap(indexName3),
            false,
        )

        // Verify log-test-001 is NOT managed (excluded)
        assertPredicatesOnMetaData(
            listOf(
                indexName2 to
                    listOf(
                        explainResponseOpendistroPolicyIdSetting to
                            fun(policyID: Any?): Boolean = policyID == null,
                        explainResponseOpenSearchPolicyIdSetting to
                            fun(policyID: Any?): Boolean = policyID == null,
                        ManagedIndexMetaData.ENABLED to
                            fun(enabled: Any?): Boolean = enabled == null,
                    ),
            ),
            getExplainMap(indexName2),
            true,
        )
        assertNull("log-test-001 should NOT be managed (excluded by pattern)", getManagedIndexConfig(indexName2))

        // Verify log-production-debug-001 is NOT managed (excluded)
        assertPredicatesOnMetaData(
            listOf(
                indexName4 to
                    listOf(
                        explainResponseOpendistroPolicyIdSetting to
                            fun(policyID: Any?): Boolean = policyID == null,
                        explainResponseOpenSearchPolicyIdSetting to
                            fun(policyID: Any?): Boolean = policyID == null,
                        ManagedIndexMetaData.ENABLED to
                            fun(enabled: Any?): Boolean = enabled == null,
                    ),
            ),
            getExplainMap(indexName4),
            true,
        )
        assertNull("log-production-debug-001 should NOT be managed (excluded by pattern)", getManagedIndexConfig(indexName4))
    }

    @Suppress("UNCHECKED_CAST")
    fun `test add template with invalid exclusion pattern`() {
        try {
            // Test exclusion pattern without content after -
            val ismTemp = ISMTemplate(listOf("log-*", "-"), 100, randomInstant())
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp)), "${testIndexName}_invalid_exclusion")
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()["error"] as Map<String, Any>
            val expectedReason = "Validation Failed: 1: index_pattern [-] must have content after '-' exclusion prefix;"
            assertEquals(expectedReason, actualMessage["reason"])
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test add template with only exclusion patterns`() {
        try {
            // Test exclusion pattern without any inclusion patterns
            val ismTemp = ISMTemplate(listOf("-log-test-*", "-log-debug-*"), 100, randomInstant())
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp)), "${testIndexName}_only_exclusion")
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()["error"] as Map<String, Any>
            val expectedReason = "Validation Failed: 1: index_patterns must contain at least one inclusion pattern (patterns cannot be all exclusions);"
            assertEquals(expectedReason, actualMessage["reason"])
        }
    }
}
