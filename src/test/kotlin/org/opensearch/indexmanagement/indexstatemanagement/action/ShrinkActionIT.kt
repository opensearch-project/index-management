/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.logging.log4j.LogManager
import org.junit.Assert
import org.junit.Assume
import org.junit.Before
import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING
import org.opensearch.common.settings.Settings
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.MediaTypeRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptShrinkStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForShrinkStep
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.waitFor
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import java.time.Instant
import java.time.temporal.ChronoUnit

class ShrinkActionIT : IndexStateManagementRestTestCase() {

    @Suppress("UnusedPrivateMember")
    @Before
    private fun disableJobIndexShardRelocation() {
        initializeManagedIndex()
        // Shrink ITs would sometimes fail on multi node setups because of the job scheduler index being moved between nodes,
        // descheduling the job
        updateIndexSetting(INDEX_MANAGEMENT_INDEX, "routing.allocation.enable", "none")
        // When doing remote testing, the docker image seems to keep the disk free space very low, causing the shrink action
        // to not be able to find a node to shrink onto. Lowering these watermarks avoids that.
        val request = """
            {
                "persistent": {
                    "${CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.key}": "5b",
                    "${CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.key}": "10b",
                    "${CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.key}": "15b"
                }
            }
        """.trimIndent()
        val res = client().makeRequest(
            "PUT", "_cluster/settings", emptyMap(),
            StringEntity(request, ContentType.APPLICATION_JSON),
        )
        assertEquals("Request failed", RestStatus.OK, res.restStatus())
    }

    private val testIndexName = javaClass.simpleName.lowercase()
    private val testIndexSuffix = "_shrink_test"

    fun `test basic workflow`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val shrinkAction = randomShrinkAction()
        val states = listOf(State("ShrinkState", listOf(shrinkAction), listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 11L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, null, "0", "3", "")

        assertShrinkActionRun(indexName, policyID)
    }

    private fun randomShrinkAction(): ShrinkAction {
        val newShards = 1
        val newMaxShardSize = ByteSizeValue.parseBytesSizeValue("1GB", "test")
        val newPercentageOfSourceShards = 0.5
        val choice = randomInt(2)
        val aliases = listOf(Alias("test-alias1"), Alias("test-alias2").filter(QueryBuilders.termQuery("foo", "bar")).writeIndex(true))
        val targetIndexTemplate = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{{ctx.index}}$testIndexSuffix", mapOf())
        return when (choice) {
            0 -> ShrinkAction(
                numNewShards = newShards,
                maxShardSize = null,
                percentageOfSourceShards = null,
                targetIndexTemplate = targetIndexTemplate,
                aliases = aliases,
                forceUnsafe = true,
                index = 0,
            )

            1 -> ShrinkAction(
                numNewShards = null,
                maxShardSize = newMaxShardSize,
                percentageOfSourceShards = null,
                targetIndexTemplate = targetIndexTemplate,
                aliases = aliases,
                forceUnsafe = true,
                index = 0,
            )

            2 -> ShrinkAction(
                numNewShards = null,
                maxShardSize = null,
                percentageOfSourceShards = newPercentageOfSourceShards,
                targetIndexTemplate = targetIndexTemplate,
                aliases = aliases,
                forceUnsafe = true,
                index = 0,
            )

            else -> {
                error("Invalid choice")
            }
        }
    }

    private fun assertShrinkActionRun(indexName: String, policyID: String, excludeNode: String? = null) {
        insertSampleData(indexName, 3)

        // Set the index as readonly to check that the setting is preserved after the shrink finishes
        updateIndexSetting(indexName, IndexMetadata.SETTING_BLOCKS_WRITE, "true")
        logger.info("index settings: \n ${getFlatSettings(indexName)}")

        // First execution: Policy is initialized
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID)
        }

        logger.info("before attempt move shards")
        // Starts AttemptMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        val targetIndexName = indexName + testIndexSuffix
        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                targetIndexName,
                getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.targetIndexName,
            )
            assertEquals("true", getIndexBlocksWriteSetting(indexName))
            val nodeName = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
            assertNotNull("Couldn't find node to shrink onto.", nodeName)
            excludeNode ?: assertNotEquals(nodeName, excludeNode)
            val settings = getFlatSettings(indexName)
            val nodeToShrink =
                getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
            assertTrue(settings.containsKey("index.routing.allocation.require._name"))
            assertEquals(nodeToShrink, settings["index.routing.allocation.require._name"])
            assertEquals(
                AttemptMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }
        val nodeToShrink =
            getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName

        // starts WaitForMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                WaitForMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
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
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        // starts WaitForShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) {
            // one primary and one replica
            assertTrue(getIndexShards(targetIndexName).size == 2)
            assertEquals(
                WaitForShrinkStep.SUCCESS_MESSAGE,
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
            assertEquals("Write block setting was not reset after successful shrink", "true", getIndexBlocksWriteSetting(indexName))
            val aliases = getAlias(targetIndexName, "")
            assertTrue("Aliases were not added to shrunken index", aliases.containsKey("test-alias1") && aliases.containsKey("test-alias2"))
        }
    }

    fun `test allocation block picks correct node with exclude node`() {
        Assume.assumeTrue(isMultiNode)
        val nodes = getNodes()
        val indexName = "${testIndexName}_index_4"
        val policyID = "${testIndexName}_testPolicyName_4"
        val shrinkAction = randomShrinkAction()
        val states = listOf(State("ShrinkState", listOf(shrinkAction), listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 11L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, null, "0", "3", "")

        val excludedNode = nodes.iterator().next()
        logger.info("Excluded node: $excludedNode")
        updateIndexSettings(
            indexName,
            Settings.builder().put("index.routing.allocation.exclude._name", excludedNode),
        )

        assertShrinkActionRun(indexName, policyID, excludedNode)
    }

    @Suppress("UNCHECKED_CAST")
    fun `test switch aliases`() {
        val indexName = "${testIndexName}_index_4"
        val aliasToSwitch = Alias("${indexName}_alias_to_switch")
            .writeIndex(false)
            .isHidden(false)
            .filter("""{"term":{"switch":"switch"}}""")
            .routing("1")

        val aliasToOverride = Alias("${indexName}_alias_to_override")
            .writeIndex(true)
            .isHidden(false)
            .filter("""{"term":{"overridden":"overridden"}}""")
            .routing("2")

        val aliasToAdd = Alias("${indexName}_alias_to_add")
            .writeIndex(false)
            .isHidden(false)
            .filter("""{"term":{"add":"add"}}""")
            .routing("3")

        val policyID = "${testIndexName}_testPolicyName_3"

        val shrinkAction = ShrinkAction(
            numNewShards = null,
            maxShardSize = null,
            percentageOfSourceShards = 0.5,
            targetIndexTemplate = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{{ctx.index}}$testIndexSuffix", mapOf()),
            aliases = listOf(aliasToOverride, aliasToAdd),
            switchAliases = true,
            forceUnsafe = true,
            index = 0,
        )
        val states = listOf(State("ShrinkState", listOf(shrinkAction), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 11L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, null, "0", "3", "")
        changeAlias(
            index = indexName, alias = aliasToSwitch.name(), action = "add", filter = aliasToSwitch.filter(), isWriteIndex = aliasToSwitch.writeIndex(), isHidden = aliasToSwitch.isHidden,
            routing = aliasToSwitch.indexRouting().toInt(), indexRouting = aliasToSwitch.indexRouting().toInt(), searchRouting = aliasToSwitch.searchRouting().toInt(),
        )
        changeAlias(
            index = indexName, alias = aliasToOverride.name(), action = "add", filter = aliasToOverride.filter(), isWriteIndex = false, isHidden = aliasToOverride.isHidden,
            routing = aliasToOverride.indexRouting().toInt(), indexRouting = aliasToOverride.indexRouting().toInt(), searchRouting = aliasToOverride.searchRouting().toInt(),
        )

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
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        val nodeToShrink = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName

        // starts WaitForMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                WaitForMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }
        // Wait for move should finish before this. Starts AttemptShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(50)) {
            assertTrue("Target index is not created", indexExists(targetIndexName))
            assertEquals(
                AttemptShrinkStep.getSuccessMessage(targetIndexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        // starts WaitForShrinkStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) {
            val sourceIndexAliases = getAlias(indexName, "")
            assertTrue("Source index aliases list must be empty after alias switch.", sourceIndexAliases.isEmpty())

            val targetIndexAliases = getAlias(targetIndexName, "")
            assertEquals("Target index aliases count is incorrect.", 3, targetIndexAliases.size)

            assertTrue("Target index must contain shrink action alias.", targetIndexAliases.containsKey(aliasToAdd.name()))
            assertAliasesEqual(aliasToAdd, targetIndexAliases[aliasToAdd.name()])

            assertTrue("Target index must contain switched source index alias.", targetIndexAliases.containsKey(aliasToSwitch.name()))
            assertAliasesEqual(aliasToSwitch, targetIndexAliases[aliasToSwitch.name()])

            assertTrue("Target index must contain shrink action alias which overrides source index alias.", targetIndexAliases.containsKey(aliasToOverride.name()))
            assertAliasesEqual(aliasToOverride, targetIndexAliases[aliasToOverride.name()])
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertAliasesEqual(expectedAlas: Alias, actualAliasRaw: Any?) {
        Assert.assertNotNull("Actual alias to compare must not be null.", actualAliasRaw)
        val actualAlias = actualAliasRaw as Map<String, Any?>
        assertEquals(expectedAlas.writeIndex() ?: false, actualAlias["is_write_index"] ?: false)
        assertEquals(expectedAlas.isHidden ?: false, actualAlias["is_hidden"] ?: false)
        assertEquals(expectedAlas.searchRouting(), actualAlias["search_routing"])
        assertEquals(expectedAlas.indexRouting(), actualAlias["index_routing"])

        val builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON)
        builder.map(actualAlias["filter"] as Map<String, Any>)
        val actualFilter = builder.toString()
        assertEquals(expectedAlas.filter(), actualFilter)
    }

    fun `test no-op with single source index primary shard`() {
        val logger = LogManager.getLogger(::ShrinkActionIT)
        val indexName = "${testIndexName}_index_shard_noop"
        val policyID = "${testIndexName}_testPolicyName_shard_noop"

        // Create a Policy with one State that only preforms a force_merge Action
        val shrinkAction = ShrinkAction(
            numNewShards = null,
            maxShardSize = null,
            percentageOfSourceShards = 0.5,
            targetIndexTemplate = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{{ctx.index}}$testIndexSuffix", mapOf()),
            aliases = null,
            forceUnsafe = true,
            index = 0,
        )
        val states = listOf(State("ShrinkState", listOf(shrinkAction), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 11L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
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
                metadata.info?.get("message"),
            )
            assertEquals(
                "Was not on the last step after no-op due to single primary shard",
                WaitForShrinkStep.name,
                metadata.stepMetaData?.name,
            )
        }
    }

    fun `test shrink with replicas`() {
        Assume.assumeTrue(isMultiNode)

        val indexName = "${testIndexName}_with_replicas"
        val policyID = "${testIndexName}_with_replicas"
        val shrinkAction = randomShrinkAction()
        val states = listOf(State("ShrinkState", listOf(shrinkAction), listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 11L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID, null, "1", "3", "")

        assertShrinkActionRun(indexName, policyID)
    }

    fun `test retries from first step`() {
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
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }
        var nodeToShrink = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
        // starts WaitForMoveShardsStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                WaitForMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
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
                getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties,
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
            nodeToShrink = getExplainManagedIndexMetaData(indexName).actionMetaData!!.actionProperties!!.shrinkActionProperties!!.nodeName
            assertTrue("Did not set allocation setting", settings.containsKey("index.routing.allocation.require._name"))
            assertTrue(settings.containsKey("index.routing.allocation.require._name"))
            assertEquals(nodeToShrink, settings["index.routing.allocation.require._name"])
            assertEquals(
                AttemptMoveShardsStep.getSuccessMessage(nodeToShrink),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }
    }
}
