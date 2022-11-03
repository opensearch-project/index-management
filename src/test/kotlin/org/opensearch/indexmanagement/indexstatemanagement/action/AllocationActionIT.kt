/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.junit.Assume
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.allocation.AttemptAllocationStep
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class AllocationActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test basic`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = AllocationAction(require = mapOf("box_type" to "hot"), exclude = emptyMap(), include = emptyMap(), index = 0)
        val states = listOf(
            State("Allocate", listOf(actionConfig), listOf())
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
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val settings = getFlatSettings(indexName)
            assertTrue(settings.containsKey("index.routing.allocation.require.box_type"))
            assertEquals(actionConfig.require["box_type"], settings["index.routing.allocation.require.box_type"])
        }
    }

    fun `test allocate require`() {
        Assume.assumeTrue(isMultiNode)

        val availableNodes = getNodes()

        val indexName = "${testIndexName}_multinode_require"
        val policyID = "${testIndexName}_multinode_require"
        createIndex(indexName, null, null, "0")

        availableNodes.remove(getIndexShardNodes(indexName)[0])

        val actionConfig = AllocationAction(require = mapOf("_name" to availableNodes.first()), exclude = emptyMap(), include = emptyMap(), index = 0)
        val states = listOf(
            State("Allocate", listOf(actionConfig), listOf())
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
        addPolicyToIndex(policyID, indexName)
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val settings = getFlatSettings(indexName)
            assertTrue(settings.containsKey("index.routing.allocation.require._name"))
            assertEquals(actionConfig.require["_name"], settings["index.routing.allocation.require._name"])
        }

        // Third execution: Waits for allocation to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertEquals(availableNodes.first(), getIndexShardNodes(indexName)[0])
        }
    }

    fun `test allocate exclude`() {
        Assume.assumeTrue(isMultiNode)

        val indexName = "${testIndexName}_multinode_exclude"
        val policyID = "${testIndexName}_multinode_exclude"

        createIndex(indexName, null, null, "0")

        val excludedNode = getIndexShardNodes(indexName)[0].toString()

        val actionConfig = AllocationAction(require = emptyMap(), exclude = mapOf("_name" to excludedNode), include = emptyMap(), index = 0)
        val states = listOf(
            State("Allocate", listOf(actionConfig), listOf())
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
        addPolicyToIndex(policyID, indexName)
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val settings = getFlatSettings(indexName)
            assertTrue(settings.containsKey("index.routing.allocation.exclude._name"))
            assertEquals(actionConfig.exclude["_name"], settings["index.routing.allocation.exclude._name"])
        }

        // Third execution: Waits for allocation to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertNotEquals(excludedNode, getIndexShardNodes(indexName)[0])
        }
    }

    fun `test allocate include`() {
        Assume.assumeTrue(isMultiNode)

        val availableNodes = getNodes()

        val indexName = "${testIndexName}_multinode_include"
        val policyID = "${testIndexName}_multinode_include"
        createIndex(indexName, null, null, "0")

        availableNodes.remove(getIndexShardNodes(indexName)[0])

        val actionConfig = AllocationAction(require = emptyMap(), exclude = emptyMap(), include = mapOf("_name" to availableNodes.first()), index = 0)
        val states = listOf(
            State("Allocate", listOf(actionConfig), listOf())
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
        addPolicyToIndex(policyID, indexName)
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val settings = getFlatSettings(indexName)
            assertTrue(settings.containsKey("index.routing.allocation.include._name"))
            assertEquals(actionConfig.include["_name"], settings["index.routing.allocation.include._name"])
        }

        // Third execution: Waits for allocation to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertEquals(availableNodes.first(), getIndexShardNodes(indexName)[0])
        }
    }

    fun `test fail on illegal key`() {
        val indexName = "${testIndexName}_illegal_key"
        val policyID = "${testIndexName}_illegal_key"
        val actionConfig = AllocationAction(require = mapOf("..//" to "value"), exclude = emptyMap(), include = emptyMap(), index = 0)
        val states = listOf(
            State("Allocate", listOf(actionConfig), listOf())
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
        createIndex(indexName, policyID, null, "0")
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertEquals(AttemptAllocationStep.getFailedMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message"))
        }
    }
}
