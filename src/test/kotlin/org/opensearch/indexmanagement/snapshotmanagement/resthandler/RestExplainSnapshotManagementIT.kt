/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.WorkflowMetadata.Companion.TRIGGER_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import java.time.Instant.now
import java.time.temporal.ChronoUnit

@Suppress("UNCHECKED_CAST")
class RestExplainSnapshotManagementIT : SnapshotManagementRestTestCase() {

    fun `test explaining a snapshot management policy`() {
        val smPolicy = createSMPolicy(
            randomSMPolicy().copy(
                jobEnabled = true,
                jobEnabledTime = now(),
                jobSchedule = IntervalSchedule(now(), 1, ChronoUnit.MINUTES),
            )
        )
        updateSMPolicyStartTime(smPolicy)
        waitFor(timeout = timeout) {
            val explainResponse = explainSMPolicy(smPolicy.policyName)
            val responseMap = createParser(XContentType.JSON.xContent(), explainResponse.entity.content).map() as Map<String, Any>
            assertTrue(responseMap.containsKey(ExplainSMPolicyResponse.SM_POLICIES_FIELD))
            val responseMapPolicies = responseMap[ExplainSMPolicyResponse.SM_POLICIES_FIELD] as List<Map<String, Any>>
            val responseMapPolicyNames = responseMapPolicies.map { it["name"] }
            assertTrue(responseMapPolicyNames.contains(smPolicy.policyName))
            val explainPolicyMap = responseMapPolicies.find { it["name"] == smPolicy.policyName } as Map<String, Any>

            val updatedPolicy = getSMPolicy(smPolicy.policyName)
            assertTrue("Explain response did not contain policy creation details", explainPolicyMap.containsKey(SMMetadata.CREATION_FIELD))
            val creationField = explainPolicyMap[SMMetadata.CREATION_FIELD] as Map<String, Any>
            val creationTriggerField = creationField[TRIGGER_FIELD] as Map<String, Any>
            val expectedCreationTime = smPolicy.creation.schedule.getNextExecutionTime(now()).toEpochMilli()
            assertEquals("Policy creation trigger time didn't match", expectedCreationTime, creationTriggerField[SMMetadata.Trigger.TIME_FIELD])
            val deletionField = explainPolicyMap[SMMetadata.DELETION_FIELD] as Map<String, Any>
            val deletionTriggerField = deletionField[TRIGGER_FIELD] as Map<String, Any>
            val expectedDeletionTime = smPolicy.deletion!!.schedule.getNextExecutionTime(now()).toEpochMilli()
            assertEquals("Policy deletion trigger time didn't match", expectedDeletionTime, deletionTriggerField[SMMetadata.Trigger.TIME_FIELD])
            assertTrue("Explain response did not contain policy sequence number", explainPolicyMap.containsKey(SMMetadata.POLICY_SEQ_NO_FIELD))
            assertEquals("Policy sequence numbers didn't match", updatedPolicy.seqNo, (explainPolicyMap[SMMetadata.POLICY_SEQ_NO_FIELD] as Int).toLong())
            assertTrue("Explain response did not contain policy primary term", explainPolicyMap.containsKey(SMMetadata.POLICY_PRIMARY_TERM_FIELD))
            assertEquals("Policy sequence numbers didn't match", updatedPolicy.primaryTerm, (explainPolicyMap[SMMetadata.POLICY_PRIMARY_TERM_FIELD] as Int).toLong())
        }
    }

    fun `test explaining a snapshot management policy which doesn't exist`() {
        val explainResponse = explainSMPolicy("nonexistent-policy")
        val responseMap = createParser(XContentType.JSON.xContent(), explainResponse.entity.content).map() as Map<String, Any>
        assertTrue(responseMap.containsKey(ExplainSMPolicyResponse.SM_POLICIES_FIELD))
        val responseMapPolicies = responseMap[ExplainSMPolicyResponse.SM_POLICIES_FIELD] as List<Map<String, Any>>
        assertTrue("Explain response map should be empty when no policies are present", responseMapPolicies.isEmpty())
    }

    fun `test explain all with list of policy names`() {
        val smPolicies = randomList(2, 3) {
            createSMPolicy(
                randomSMPolicy(
                    jobEnabled = true,
                )
            )
        }
        // if this proves to be flaky, just index the metadata directly instead of executing to generate metadata
        smPolicies.forEach { updateSMPolicyStartTime(it) }
        waitFor(timeout = timeout) {
            val explainResponse = explainSMPolicy(smPolicies.joinToString(",") { it.policyName })
            val responseMap = createParser(XContentType.JSON.xContent(), explainResponse.entity.content).map() as Map<String, Any>
            assertTrue(responseMap.containsKey(ExplainSMPolicyResponse.SM_POLICIES_FIELD))
            val responseMapPolicies = responseMap[ExplainSMPolicyResponse.SM_POLICIES_FIELD] as List<Map<String, Any>>
            val responseMapPolicyNames = responseMapPolicies.map { it["name"] }

            smPolicies.forEach { actualPolicy ->
                assertTrue(responseMapPolicyNames.contains(actualPolicy.policyName))
                val foundPolicy = responseMapPolicies.find { it["name"] == actualPolicy.policyName } as Map<String, Any>
                assertTrue(foundPolicy.containsKey(SMPolicy.ENABLED_FIELD))
                assertEquals(actualPolicy.jobEnabled, foundPolicy[SMPolicy.ENABLED_FIELD])
                assertTrue(foundPolicy.containsKey(SMMetadata.CREATION_FIELD))
                assertTrue(foundPolicy.containsKey(SMMetadata.DELETION_FIELD))
            }
        }
    }

    fun `test explain all with empty policy name`() {
        val smPolicies = randomList(2, 3) {
            createSMPolicy(
                randomSMPolicy(
                    jobEnabled = true,
                )
            )
        }
        // if this proves to be flaky, just index the metadata directly instead of executing to generate metadata
        smPolicies.forEach { updateSMPolicyStartTime(it) }
        waitFor(timeout = timeout) {
            val explainResponse = explainSMPolicy("")
            val responseMap = createParser(XContentType.JSON.xContent(), explainResponse.entity.content).map() as Map<String, Any>
            assertTrue(responseMap.containsKey(ExplainSMPolicyResponse.SM_POLICIES_FIELD))
            val responseMapPolicies = responseMap[ExplainSMPolicyResponse.SM_POLICIES_FIELD] as List<Map<String, Any>>
            val responseMapPolicyNames = responseMapPolicies.map { it["name"] }

            smPolicies.forEach { actualPolicy ->
                assertTrue(responseMapPolicyNames.contains(actualPolicy.policyName))
                val foundPolicy = responseMapPolicies.find { it["name"] == actualPolicy.policyName } as Map<String, Any>
                logger.info("Found policy $foundPolicy") // checking flaky, creation field can be missing
                assertTrue("enabled field exists", foundPolicy.containsKey(SMPolicy.ENABLED_FIELD))
                assertEquals(actualPolicy.jobEnabled, foundPolicy[SMPolicy.ENABLED_FIELD])
                assertTrue("creation field exists", foundPolicy.containsKey(SMMetadata.CREATION_FIELD))
                assertTrue("deletion field exists", foundPolicy.containsKey(SMMetadata.DELETION_FIELD))
            }
        }
    }

    fun `test explain sm policy for wildcard id`() {
        val smPolicy1 = createSMPolicy(randomSMPolicy(policyName = "prefix_name1_suffix"))
        val smPolicy2 = createSMPolicy(randomSMPolicy(policyName = "prefix_name2_suffix"))
        waitFor {
            val explainResponse = explainSMPolicy("prefix_*_suffix")
            val responseMap = createParser(XContentType.JSON.xContent(), explainResponse.entity.content).map() as Map<String, Any>
            assertTrue(responseMap.containsKey(ExplainSMPolicyResponse.SM_POLICIES_FIELD))
            val responseMapPolicies = responseMap[ExplainSMPolicyResponse.SM_POLICIES_FIELD] as List<Map<String, Any>>
            val responseMapPolicyNames = responseMapPolicies.map { it["name"] }
            assertTrue(responseMapPolicyNames.contains(smPolicy1.policyName))
            assertTrue(responseMapPolicyNames.contains(smPolicy2.policyName))
        }
    }

    fun `test explain sm policy when config index doesn't exist`() {
        try {
            deleteIndex(INDEX_MANAGEMENT_INDEX)
            explainSMPolicy(randomAlphaOfLength(10))
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
