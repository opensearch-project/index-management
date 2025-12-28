/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class SearchOnlyActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    /**
     * Tests that the search_only action fails on an index without Remote Store and Segment Replication.
     * The _scale API requires these prerequisites to be enabled.
     */
    fun `test failure in search_only on index without remote store`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = SearchOnlyAction(0)
        val states =
            listOf(
                State("SearchOnlyState", listOf(actionConfig), listOf()),
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
            )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainResponse = getExplainManagedIndexMetaData(indexName)
            // Expecting the step to fail as Remote Store/Segment Replication are not enabled
            assertEquals(Step.StepStatus.FAILED, explainResponse.stepMetaData?.stepStatus)
            val info = explainResponse.info.toString()
            assertTrue(
                "Expected failure message about search-only prerequisites, but got: $info",
                info.contains("Cannot scale to zero without search replicas") ||
                    info.contains("search-only") ||
                    info.contains("remote") ||
                    info.contains("scale"),
            )
        }
    }
}
