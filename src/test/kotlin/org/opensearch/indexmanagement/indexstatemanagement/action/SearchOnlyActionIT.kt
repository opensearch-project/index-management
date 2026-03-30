/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.searchonly.AttemptSearchOnlyStep
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class SearchOnlyActionIT : IndexStateManagementRestTestCase() {

    // Remote Store system repositories cannot be deleted
    override fun preserveReposUponCompletion(): Boolean = true

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test search_only action on remote store index`() {
        val indexName = "${testIndexName}_index"
        val policyID = "${testIndexName}_policy"

        val searchOnlyAction = SearchOnlyAction(index = 0)

        val state = State(
            name = "searchOnlyState",
            actions = listOf(searchOnlyAction),
            transitions = listOf(),
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = state.name,
            states = listOf(state),
        )

        createPolicy(policy, policyID)

        // Create index with search replicas (required for search_only validation)
        createIndex(
            indexName,
            policyID,
            replicas = "0",
            settings = Settings.builder()
                .put("index.number_of_search_replicas", 1)
                .build(),
        )

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Initialize policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                "Successfully initialized policy: $policyID",
                explainMetaData.info?.get("message"),
            )
        }

        // Execute search_only action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                AttemptSearchOnlyStep.getSuccessMessage(indexName),
                explainMetaData.info?.get("message"),
            )
        }
    }
}
