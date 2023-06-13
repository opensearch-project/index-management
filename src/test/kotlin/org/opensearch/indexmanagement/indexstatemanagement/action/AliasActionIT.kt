/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.alias.AttemptAliasActionsStep
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class AliasActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test adding alias to index`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val aliasName = "some_alias_to_add"
        val actions = listOf(IndicesAliasesRequest.AliasActions.add().alias(aliasName))
        val actionConfig = AliasAction(actions = actions, index = 0)
        val states = listOf(State("alias", listOf(actionConfig), listOf()))

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
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName)!!.policyID) }

        waitFor {
            val alias = getAlias(indexName, "")
            assertTrue("Alias was already added to index", !alias.containsKey(aliasName))
        }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val info = getExplainManagedIndexMetaData(indexName)!!.info as Map<String, Any?>
            assertEquals("Alias was not successfully updated", AttemptAliasActionsStep.getSuccessMessage(indexName), info["message"])
            val alias = getAlias(indexName, "")
            assertTrue("Alias was not added to index", alias.containsKey(aliasName))
        }
    }

    fun `test adding alias to index using ctx`() {
        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"
        val aliasName = "some_alias_to_add"
        val actions = listOf(IndicesAliasesRequest.AliasActions.add().alias(aliasName))
        val actionConfig = AliasAction(actions = actions, index = 0)
        val states = listOf(State("alias", listOf(actionConfig), listOf()))

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
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName)!!.policyID) }

        waitFor {
            val alias = getAlias(indexName, "")
            assertTrue("Alias was already added to index", !alias.containsKey(aliasName))
        }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val info = getExplainManagedIndexMetaData(indexName)!!.info as Map<String, Any?>
            assertEquals("Alias was not successfully updated", AttemptAliasActionsStep.getSuccessMessage(indexName), info["message"])
            val alias = getAlias(indexName, "")
            assertTrue("Alias was not added to index", alias.containsKey(aliasName))
        }
    }

    fun `test removing alias from index`() {
        val indexName = "${testIndexName}_index_3"
        val policyID = "${testIndexName}_testPolicyName_3"
        val aliasName = "some_alias_to_remove"
        val actions = listOf(IndicesAliasesRequest.AliasActions.remove().alias(aliasName))
        val actionConfig = AliasAction(actions = actions, index = 0)
        val states = listOf(State("alias", listOf(actionConfig), listOf()))

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
        createIndex(indexName, policyID, aliasName)
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName)!!.policyID) }

        waitFor {
            val alias = getAlias(indexName, "")
            assertTrue("Alias was not added to index", alias.containsKey(aliasName))
        }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val info = getExplainManagedIndexMetaData(indexName)!!.info as Map<String, Any?>
            assertEquals("Alias was not successfully updated", AttemptAliasActionsStep.getSuccessMessage(indexName), info["message"])
            val alias = getAlias(indexName, "")
            assertTrue("Alias was not removed from index", !alias.containsKey(aliasName))
        }
    }
}
