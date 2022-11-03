/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class DeleteActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test basic`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = DeleteAction(0)
        val states = listOf(
            State("DeleteState", listOf(actionConfig), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        waitFor { assertIndexExists(indexName) }

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // confirm index does not exist anymore
        waitFor { assertIndexDoesNotExist(indexName) }

        // TODO flaky after we delete the managed index, there could be race condition that causing
        //  update metadata fail because metadata has been deleted after index deleted
        //  and update metadata fail causing history to not be updated
        // confirm we added a history document that says we did a successful delete operation
        // waitFor {
        //     val response = getHistorySearchResponse(indexName)
        //     assertTrue(
        //         response.hits.hits
        //             .map { it.sourceAsMap }
        //             .any {
        //                 val metadata = it["managed_index_meta_data"] as Map<*, *>
        //                 val index = metadata["index"] as String
        //                 if (metadata.containsKey("action")) {
        //                     val action = metadata["action"] as Map<*, *>
        //                     val actionName = action["name"] as String
        //                     val step = metadata["step"] as Map<*, *>
        //                     val stepName = step["name"] as String
        //                     val stepStatus = step["step_status"] as String
        //                     index == indexName && actionName == "delete" && stepName == "attempt_delete" && stepStatus == "completed"
        //                 } else {
        //                     false
        //                 }
        //             }
        //     )
        // }
    }
}
