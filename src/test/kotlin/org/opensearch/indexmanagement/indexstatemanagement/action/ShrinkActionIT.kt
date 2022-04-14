/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptShrinkStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForShrinkStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.waitFor
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import java.time.Instant
import java.time.temporal.ChronoUnit

class ShrinkActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase()
    private val testIndexSuffix = "_shrink_test"
    fun `test basic workflow number of shards`() {
        val logger = LogManager.getLogger(::ShrinkActionIT)
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"

        val shrinkAction = ShrinkAction(
            numNewShards = 1,
            maxShardSize = null,
            percentageOfSourceShards = null,
            targetIndexTemplate = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{{ctx.index}}$testIndexSuffix", mapOf()),
            aliases = null,
            forceUnsafe = true,
            index = 0
        )
        val states = listOf(State("ShrinkState", listOf(shrinkAction), listOf()))

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

        // Set the index as readonly to check that the setting is preserved after the shrink finishes
        updateIndexSetting(indexName, IndexMetadata.SETTING_BLOCKS_WRITE, "true")

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        logger.info("before attempt move shards")
        // Starts AttemptMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val targetIndexName = indexName + testIndexSuffix
        waitFor(Instant.ofEpochSecond(60)) {
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
        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                WaitForMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
        // Wait for move should finish before this. Starts AttemptShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        val instant: Instant = Instant.ofEpochSecond(50)
        waitFor(instant) {
            assertTrue("Target index is not created", indexExists(targetIndexName))
            assertEquals(Step.StepStatus.COMPLETED, getExplainManagedIndexMetaData(indexName).stepMetaData?.stepStatus)
            assertEquals(
                AttemptShrinkStep.getSuccessMessage(targetIndexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // starts WaitForShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) {
            // one primary and one replica
            assertTrue(getIndexShards(targetIndexName).size == 2)
            assertEquals(
                WaitForShrinkStep.SUCCESS_MESSAGE,
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
            assertEquals("Write block setting was not reset after successful shrink", "true", getIndexBlocksWriteSetting(indexName))
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test basic workflow max shard size`() {
        val logger = LogManager.getLogger(::ShrinkActionIT)
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val testMaxShardSize: ByteSizeValue = ByteSizeValue.parseBytesSizeValue("1GB", "test")
        val shrinkAction = ShrinkAction(
            numNewShards = null,
            maxShardSize = testMaxShardSize,
            percentageOfSourceShards = null,
            targetIndexTemplate = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{{ctx.index}}$testIndexSuffix", mapOf()),
            aliases = null,
            forceUnsafe = true,
            index = 0
        )
        val states = listOf(State("ShrinkState", listOf(shrinkAction), listOf()))

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
        waitFor(Instant.ofEpochSecond(60)) { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        logger.info("before attempt move shards")
        // Starts AttemptMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val targetIndexName = indexName + testIndexSuffix
        waitFor(Instant.ofEpochSecond(60)) {
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
        waitFor(Instant.ofEpochSecond(60)) {
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
        waitFor(Instant.ofEpochSecond(60)) {
            // one primary and one replica
            assertTrue(getIndexShards(targetIndexName).size == 2)
            assertEquals(
                WaitForShrinkStep.SUCCESS_MESSAGE,
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
            val indexSettings = getIndexSettings(indexName) as Map<String, Map<String, Map<String, Any?>>>
            val writeBlock = indexSettings[indexName]!!["settings"]!![IndexMetadata.SETTING_BLOCKS_WRITE] as String?
            assertNull("Write block setting was not reset after successful shrink", writeBlock)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test basic workflow percentage to decrease to`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val shrinkAction = ShrinkAction(
            numNewShards = null,
            maxShardSize = null,
            percentageOfSourceShards = 0.5,
            targetIndexTemplate = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{{ctx.index}}$testIndexSuffix", mapOf()),
            aliases = null,
            forceUnsafe = true,
            index = 0
        )
        val states = listOf(State("ShrinkState", listOf(shrinkAction), listOf()))

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
        waitFor(Instant.ofEpochSecond(60)) { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        // Starts AttemptMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val targetIndexName = indexName + testIndexSuffix
        waitFor(Instant.ofEpochSecond(60)) {
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
        waitFor(Instant.ofEpochSecond(60)) {
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
        waitFor(Instant.ofEpochSecond(60)) {
            // one primary and one replica
            assertTrue(getIndexShards(targetIndexName).size == 2)
            assertEquals(
                WaitForShrinkStep.SUCCESS_MESSAGE,
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
            val indexSettings = getIndexSettings(indexName) as Map<String, Map<String, Map<String, Any?>>>
            val writeBlock = indexSettings[indexName]!!["settings"]!![IndexMetadata.SETTING_BLOCKS_WRITE] as String?
            assertNull("Write block setting was not reset after successful shrink", writeBlock)
        }
    }

    fun `test allocation block picks correct node`() {
        val logger = LogManager.getLogger(::ShrinkActionIT)
        val nodes = getNodes()
        if (nodes.size > 1) {
            val indexName = "${testIndexName}_index_1"
            val policyID = "${testIndexName}_testPolicyName_1"
            val shrinkAction = ShrinkAction(
                numNewShards = null,
                maxShardSize = null,
                percentageOfSourceShards = 0.5,
                targetIndexTemplate = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{{ctx.index}}$testIndexSuffix", mapOf()),
                aliases = null,
                forceUnsafe = true,
                index = 0
            )
            val states = listOf(State("ShrinkState", listOf(shrinkAction), listOf()))

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
            waitFor(Instant.ofEpochSecond(60)) { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
            // Starts AttemptMoveShardsStep
            updateManagedIndexConfigStartTime(managedIndexConfig)
            val targetIndexName = indexName + testIndexSuffix
            waitFor(Instant.ofEpochSecond(60)) {
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
            waitFor(Instant.ofEpochSecond(60)) {
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
            waitFor(Instant.ofEpochSecond(60)) {
                // one primary and one replica
                assertTrue(getIndexShards(targetIndexName).size == 2)
                assertEquals(
                    WaitForShrinkStep.SUCCESS_MESSAGE,
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
                )
                assertEquals("Write block setting was not reset after successful shrink", "false", getIndexBlocksWriteSetting(indexName))
            }
        }
    }

    fun `test no-op with single source index primary shard`() {
        val logger = LogManager.getLogger(::ShrinkActionIT)
        val indexName = "${testIndexName}_index_1_shard_noop"
        val policyID = "${testIndexName}_testPolicyName_1_shard_noop"

        // Create a Policy with one State that only preforms a force_merge Action
        val shrinkAction = ShrinkAction(
            numNewShards = null,
            maxShardSize = null,
            percentageOfSourceShards = 0.5,
            targetIndexTemplate = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{{ctx.index}}$testIndexSuffix", mapOf()),
            aliases = null,
            forceUnsafe = true,
            index = 0
        )
        val states = listOf(State("ShrinkState", listOf(shrinkAction), listOf()))

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
        createIndex(indexName, policyID, null, "0", "1", "")

        insertSampleData(indexName, 3)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        logger.info("before attempt move shards")
        // Starts AttemptMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // The action should be done after the no-op
        waitFor(Instant.ofEpochSecond(60)) {
            val metadata = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                "Did not get the no-op due to single primary shard message",
                AttemptMoveShardsStep.ONE_PRIMARY_SHARD_MESSAGE,
                metadata.info?.get("message")
            )
            assertEquals(
                "Was not on the last step after no-op due to single primary shard",
                WaitForShrinkStep.name,
                metadata.stepMetaData?.name
            )
        }
    }

    // TODO This test is excessively flaky, disabling for now but it needs to be fixed
    private fun `test retries from first step`() {
        val testPolicy = """
        {"policy":{"description":"Default policy","default_state":"Shrink","states":[
        {"name":"Shrink","actions":[{"retry":{"count":2,"backoff":"constant","delay":"1s"},"shrink":
        {"num_new_shards":1, "target_index_name_template":{"source": "{{ctx.index}}_shrink_test"}, "force_unsafe": "true"}}],"transitions":[]}]}}
        """.trimIndent()
        val logger = LogManager.getLogger(::ShrinkActionIT)
        val indexName = "${testIndexName}_retry"
        val policyID = "${testIndexName}_testPolicyName_retry"
        createPolicyJson(testPolicy, policyID)

        createIndex(indexName, policyID, null, "0", "3", "")
        insertSampleData(indexName, 3)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        logger.info("before attempt move shards")
        // Starts AttemptMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val targetIndexName = indexName + "_shrink_test"
        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(targetIndexName, getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.targetIndexName)
            assertNotNull("Couldn't find node to shrink onto.", getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName)
            val settings = getFlatSettings(indexName)
            val nodeToShrink = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
            assertTrue("Did not set allocation setting", settings.containsKey("index.routing.allocation.require._name"))
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
        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                WaitForMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
        // Create an index with the target index name so the AttemptShrinkStep fails
        createIndex(targetIndexName, null)

        // Wait for move should finish before this. Starts AttemptShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) {
            val stepMetadata = getExplainManagedIndexMetaData(indexName).stepMetaData
            assertEquals("Did not fail due to target index existing step as expected", Step.StepStatus.FAILED, stepMetadata?.stepStatus)
            assertEquals(AttemptShrinkStep.name, stepMetadata?.name)
            val settings = getFlatSettings(indexName)
            assertFalse("Did not clear allocation setting", settings.containsKey("index.routing.allocation.require._name"))
            assertFalse("Did not clear index write block setting.", settings.containsKey("index.blocks.writes"))
            assertNull(
                "Did not clear shrink action properties",
                getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties
            )
        }

        // wait 5 seconds for the timeout from the retry to pass
        Thread.sleep(5000L)

        // Delete that index so it can pass
        deleteIndex(targetIndexName)

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) {
            val stepMetadata = getExplainManagedIndexMetaData(indexName).stepMetaData
            assertEquals("Shrink action should have started over after failing", stepMetadata?.name, AttemptMoveShardsStep.name)
            // The step status should be starting, but in the same execution will be completed. Allowing either to avoid flaky failures
            val stepStatusDidReset = stepMetadata?.stepStatus == Step.StepStatus.STARTING || stepMetadata?.stepStatus == Step.StepStatus.COMPLETED
            assertTrue("Step status should reset", stepStatusDidReset)
        }

        waitFor {
            assertEquals(targetIndexName, getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.targetIndexName)
            assertNotNull("Couldn't find node to shrink onto.", getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName)
            val settings = getFlatSettings(indexName)
            assertTrue("Did not set allocation setting", settings.containsKey("index.routing.allocation.require._name"))
            assertTrue(settings.containsKey("index.routing.allocation.require._name"))
            assertEquals(nodeToShrink, settings["index.routing.allocation.require._name"])
            assertEquals(
                AttemptMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }
}
