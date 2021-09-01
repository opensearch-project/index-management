package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.logging.log4j.LogManager
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ShrinkActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptShrinkStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForShrinkStep
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ShrinkActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)
    fun `test basic workflow number of shards`() {
        val logger = LogManager.getLogger(::ShrinkActionIT)
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"

        // Create a Policy with one State that only preforms a force_merge Action
        val shrinkActionConfig = ShrinkActionConfig(
            numNewShards = 1,
            maxShardSize = null,
            percentageDecrease = null,
            targetIndexSuffix = "_shrink_test",
            aliases = null,
            forceUnsafe = true,
            index = 0
        )
        val states = listOf(State("ShrinkState", listOf(shrinkActionConfig), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 11L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, null, "0", "3", "")

        insertSampleData(indexName, 3)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        logger.info("before attempt move shards")
        // Starts AttemptMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val targetIndexName = indexName + "_shrink_test"
        waitFor {
            assertEquals(targetIndexName, getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.targetIndexName)
            assertEquals("true", getIndexBlocksWriteSetting(indexName))
            assertNotNull("Couldn't find node to shrink onto.", getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName)
            val settings = getFlatSettings(indexName)
            val nodeToShrink = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
            assertTrue(settings.containsKey("index.routing.allocation.require._name"))
            assertEquals(nodeToShrink, settings["index.routing.allocation.require._name"])
            assertEquals(
                AttemptMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
        val nodeToShrink = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
        // starts WaitForMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
        // Wait for move should finish before this. Starts AttemptShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        val instant: Instant = Instant.ofEpochSecond(50)
        waitFor(instant) {
            // assertTrue("Target index is not created", indexExists(targetIndexName))
            assertEquals(Step.StepStatus.COMPLETED, getExplainManagedIndexMetaData(indexName).stepMetaData?.stepStatus)
            assertEquals(
                AttemptShrinkStep.getSuccessMessage(targetIndexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // starts WaitForShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            // one primary and one replica
            assertTrue(getIndexShards(targetIndexName).size == 2)
            assertEquals(
                WaitForShrinkStep.getSuccessMessage(),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }

    fun `test basic workflow max shard size`() {
        val logger = LogManager.getLogger(::ShrinkActionIT)
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val testMaxShardSize: ByteSizeValue = ByteSizeValue.parseBytesSizeValue("1GB", "test")
        // Create a Policy with one State that only preforms a force_merge Action
        val shrinkActionConfig = ShrinkActionConfig(
            numNewShards = null,
            maxShardSize = testMaxShardSize,
            percentageDecrease = null,
            targetIndexSuffix = "_shrink_test",
            aliases = null,
            forceUnsafe = true,
            index = 0
        )
        val states = listOf(State("ShrinkState", listOf(shrinkActionConfig), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 11L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, null, "0", "3", "")

        insertSampleData(indexName, 3)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        logger.info("before attempt move shards")
        // Starts AttemptMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val targetIndexName = indexName + "_shrink_test"
        waitFor {
            assertEquals(targetIndexName, getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.targetIndexName)
            assertEquals("true", getIndexBlocksWriteSetting(indexName))
            assertNotNull("Couldn't find node to shrink onto.", getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName)
            val settings = getFlatSettings(indexName)
            val nodeToShrink = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
            assertTrue(settings.containsKey("index.routing.allocation.require._name"))
            assertEquals(nodeToShrink, settings["index.routing.allocation.require._name"])
            assertEquals(
                AttemptMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
        val nodeToShrink = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
        // starts WaitForMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
        // Wait for move should finish before this. Starts AttemptShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(50)) {
            assertTrue("Target index is not created", indexExists(targetIndexName))
            assertEquals(
                AttemptShrinkStep.getSuccessMessage(targetIndexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // starts WaitForShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            // one primary and one replica
            assertTrue(getIndexShards(targetIndexName).size == 2)
            assertEquals(
                WaitForShrinkStep.getSuccessMessage(),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }

    fun `test basic workflow percentage decrease`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        // Create a Policy with one State that only preforms a force_merge Action
        val shrinkActionConfig = ShrinkActionConfig(
            numNewShards = null,
            maxShardSize = null,
            percentageDecrease = 0.5,
            targetIndexSuffix = "_shrink_test",
            aliases = null,
            forceUnsafe = true,
            index = 0
        )
        val states = listOf(State("ShrinkState", listOf(shrinkActionConfig), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 11L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, null, "0", "3", "")

        insertSampleData(indexName, 3)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        // Starts AttemptMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val targetIndexName = indexName + "_shrink_test"
        waitFor {
            assertEquals(targetIndexName, getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.targetIndexName)
            assertEquals("true", getIndexBlocksWriteSetting(indexName))
            assertNotNull("Couldn't find node to shrink onto.", getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName)
            val settings = getFlatSettings(indexName)
            val nodeToShrink = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
            assertTrue(settings.containsKey("index.routing.allocation.require._name"))
            assertEquals(nodeToShrink, settings["index.routing.allocation.require._name"])
            assertEquals(
                AttemptMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        val nodeToShrink = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName

        // starts WaitForMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
        // Wait for move should finish before this. Starts AttemptShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(50)) {
            assertTrue("Target index is not created", indexExists(targetIndexName))
            assertEquals(
                AttemptShrinkStep.getSuccessMessage(targetIndexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // starts WaitForShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            // one primary and one replica
            assertTrue(getIndexShards(targetIndexName).size == 2)
            assertEquals(
                WaitForShrinkStep.getSuccessMessage(),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }

    fun `test allocation block picks correct node`() {
        val logger = LogManager.getLogger(::ShrinkActionIT)
        val nodes = getNodes()
        if (nodes.size > 1) {
            val indexName = "${testIndexName}_index_1"
            val policyID = "${testIndexName}_testPolicyName_1"
            // Create a Policy with one State that only preforms a force_merge Action
            val shrinkActionConfig = ShrinkActionConfig(
                numNewShards = null,
                maxShardSize = null,
                percentageDecrease = 0.5,
                targetIndexSuffix = "_shrink_test",
                aliases = null,
                forceUnsafe = true,
                index = 0
            )
            val states = listOf(State("ShrinkState", listOf(shrinkActionConfig), listOf()))

            val policy = Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 11L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states
            )
            createPolicy(policy, policyID)
            createIndex(indexName, policyID, null, "0", "3", "")
            val excludedNode = nodes.iterator().next()
            logger.info("Excluded node: $excludedNode")
            updateIndexSettings(
                indexName,
                Settings.builder().put("index.routing.allocation.exclude._name", excludedNode)
            )
            insertSampleData(indexName, 3)
            // Will change the startTime each execution so that it triggers in 2 seconds
            // First execution: Policy is initialized
            val managedIndexConfig = getExistingManagedIndexConfig(indexName)
            logger.info("index settings: \n ${getFlatSettings(indexName)}")

            updateManagedIndexConfigStartTime(managedIndexConfig)
            waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
            // Starts AttemptMoveShardsStep
            updateManagedIndexConfigStartTime(managedIndexConfig)
            val targetIndexName = indexName + "_shrink_test"
            waitFor {
                assertEquals(
                    targetIndexName,
                    getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.targetIndexName
                )
                assertEquals("true", getIndexBlocksWriteSetting(indexName))
                val nodeName =
                    getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
                assertNotNull("Couldn't find node to shrink onto.", nodeName)
                assertNotEquals(nodeName, excludedNode)
                val settings = getFlatSettings(indexName)
                val nodeToShrink =
                    getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
                assertTrue(settings.containsKey("index.routing.allocation.require._name"))
                assertEquals(nodeToShrink, settings["index.routing.allocation.require._name"])
                assertEquals(
                    AttemptMoveShardsStep.getSuccessMessage(nodeToShrink),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
                )
            }

            val nodeToShrink =
                getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName

            // starts WaitForMoveShardsStep
            updateManagedIndexConfigStartTime(managedIndexConfig)
            waitFor {
                assertEquals(
                    WaitForMoveShardsStep.getSuccessMessage(nodeToShrink),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
                )
            }
            // Wait for move should finish before this. Starts AttemptShrinkStep
            updateManagedIndexConfigStartTime(managedIndexConfig)
            waitFor(Instant.ofEpochSecond(50)) {
                assertTrue("Target index is not created", indexExists(targetIndexName))
                assertEquals(
                    AttemptShrinkStep.getSuccessMessage(targetIndexName),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
                )
            }

            // starts WaitForShrinkStep
            updateManagedIndexConfigStartTime(managedIndexConfig)
            waitFor {
                // one primary and one replica
                assertTrue(getIndexShards(targetIndexName).size == 2)
                assertEquals(
                    WaitForShrinkStep.getSuccessMessage(),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
                )
            }
        }
    }
}
