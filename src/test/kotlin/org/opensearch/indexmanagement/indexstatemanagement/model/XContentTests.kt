/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.indexstatemanagement.action.RollupAction
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.DestinationType
import org.opensearch.indexmanagement.indexstatemanagement.nonNullRandomConditions
import org.opensearch.indexmanagement.indexstatemanagement.randomAllocationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomChannel
import org.opensearch.indexmanagement.indexstatemanagement.randomCloseActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomDeleteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomDestination
import org.opensearch.indexmanagement.indexstatemanagement.randomForceMergeActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomIndexPriorityActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomNotificationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomOpenActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomReadOnlyActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReadWriteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReplicaCountActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomRolloverActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomRollupActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.randomSnapshotActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.randomTransition
import org.opensearch.indexmanagement.indexstatemanagement.toJsonString
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.test.OpenSearchTestCase

class XContentTests : OpenSearchTestCase() {

    fun `test policy parsing`() {
        val policy = randomPolicy()

        val policyString = policy.toJsonString()
        val parsedPolicy = parserWithType(policyString).parseWithType(policy.id, policy.seqNo, policy.primaryTerm, Policy.Companion::parse)
        assertEquals("Round tripping Policy doesn't work", policy, parsedPolicy)
    }

    fun `test state parsing`() {
        val state = randomState()

        val stateString = state.toJsonString()
        val parsedState = State.parse(parser(stateString))
        assertEquals("Round tripping State doesn't work", state, parsedState)
    }

    fun `test transition parsing`() {
        val transition = randomTransition()

        val transitionString = transition.toJsonString()
        val parsedTransition = Transition.parse(parser(transitionString))
        assertEquals("Round tripping Transition doesn't work", transition, parsedTransition)
    }

    fun `test conditions parsing`() {
        val conditions = nonNullRandomConditions()

        val conditionsString = conditions.toJsonString()
        val parsedConditions = Conditions.parse(parser(conditionsString))
        assertEquals("Round tripping Conditions doesn't work", conditions, parsedConditions)
    }

    fun `test delete action parsing`() {
        val deleteAction = randomDeleteActionConfig()

        val deleteActionString = deleteAction.toJsonString()
        val parsedDeleteAction = ISMActionsParser.instance.parse(parser(deleteActionString), 0)
        assertEquals("Round tripping DeleteAction doesn't work", deleteAction.convertToMap(), parsedDeleteAction.convertToMap())
    }

    fun `test rollover action parsing`() {
        val rolloverAction = randomRolloverActionConfig()

        val rolloverActionString = rolloverAction.toJsonString()
        val parsedRolloverAction = ISMActionsParser.instance.parse(parser(rolloverActionString), 0)
        assertEquals("Round tripping RolloverAction doesn't work", rolloverAction.convertToMap(), parsedRolloverAction.convertToMap())
    }

    fun `test read_only action parsing`() {
        val readOnlyAction = randomReadOnlyActionConfig()

        val readOnlyActionString = readOnlyAction.toJsonString()
        val parsedReadOnlyAction = ISMActionsParser.instance.parse(parser(readOnlyActionString), 0)
        assertEquals("Round tripping ReadOnlyAction doesn't work", readOnlyAction.convertToMap(), parsedReadOnlyAction.convertToMap())
    }

    fun `test read_write action parsing`() {
        val readWriteAction = randomReadWriteActionConfig()

        val readWriteActionString = readWriteAction.toJsonString()
        val parsedReadWriteAction = ISMActionsParser.instance.parse(parser(readWriteActionString), 0)
        assertEquals("Round tripping ReadWriteAction doesn't work", readWriteAction.convertToMap(), parsedReadWriteAction.convertToMap())
    }

    fun `test replica_count action config parsing`() {
        val replicaCountAction = randomReplicaCountActionConfig()

        val replicaCountActionString = replicaCountAction.toJsonString()
        val parsedReplicaCountAction = ISMActionsParser.instance.parse(parser(replicaCountActionString), 0)
        assertEquals("Round tripping ReplicaCountAction doesn't work", replicaCountAction.convertToMap(), parsedReplicaCountAction.convertToMap())
    }

    fun `test set_index_priority action config parsing`() {
        val indexPriorityAction = randomIndexPriorityActionConfig()

        val indexPriorityActionString = indexPriorityAction.toJsonString()
        val parsedIndexPriorityAction = ISMActionsParser.instance.parse(parser(indexPriorityActionString), 0)
        assertEquals("Round tripping indexPriorityAction doesn't work", indexPriorityAction.convertToMap(), parsedIndexPriorityAction.convertToMap())
    }

    fun `test force_merge action config parsing`() {
        val forceMergeAction = randomForceMergeActionConfig()

        val forceMergeActionString = forceMergeAction.toJsonString()
        val parsedForceMergeAction = ISMActionsParser.instance.parse(parser(forceMergeActionString), 0)
        assertEquals("Round tripping ForceMergeAction doesn't work", forceMergeAction.convertToMap(), parsedForceMergeAction.convertToMap())
    }

    fun `test notification action parsing`() {
        val chimeNotificationAction = randomNotificationActionConfig(destination = randomDestination(type = DestinationType.CHIME))
        val slackNotificationAction = randomNotificationActionConfig(destination = randomDestination(type = DestinationType.SLACK))
        val customNotificationAction = randomNotificationActionConfig(destination = randomDestination(type = DestinationType.CUSTOM_WEBHOOK))

        val chimeNotificationActionString = chimeNotificationAction.toJsonString()
        val chimeParsedNotificationAction = ISMActionsParser.instance.parse(parser(chimeNotificationActionString), 0)
        assertEquals(
            "Round tripping chime NotificationAction doesn't work",
            chimeNotificationAction.convertToMap(), chimeParsedNotificationAction.convertToMap()
        )

        val slackNotificationActionString = slackNotificationAction.toJsonString()
        val slackParsedNotificationAction = ISMActionsParser.instance.parse(parser(slackNotificationActionString), 0)
        assertEquals(
            "Round tripping slack NotificationAction doesn't work",
            slackNotificationAction.convertToMap(), slackParsedNotificationAction.convertToMap()
        )

        val customNotificationActionString = customNotificationAction.toJsonString()
        val customParsedNotificationAction = ISMActionsParser.instance.parse(parser(customNotificationActionString), 0)
        assertEquals(
            "Round tripping custom webhook NotificationAction doesn't work",
            customNotificationAction.convertToMap(), customParsedNotificationAction.convertToMap()
        )
    }

    fun `test snapshot action config parsing`() {
        val snapshotAction = randomSnapshotActionConfig("repository", "snapshot")

        val snapshotActionString = snapshotAction.toJsonString()
        val parsedSnapshotAction = ISMActionsParser.instance.parse(parser(snapshotActionString), 0)
        assertEquals(
            "Round tripping SnapshotAction doesn't work",
            snapshotAction.convertToMap(), parsedSnapshotAction.convertToMap()
        )
    }

    fun `test allocation action config parsing`() {
        val allocationAction = randomAllocationActionConfig(
            require = mapOf("box_type" to "hot"),
            include = mapOf(randomAlphaOfLengthBetween(1, 10) to randomAlphaOfLengthBetween(1, 10)),
            exclude = mapOf(randomAlphaOfLengthBetween(1, 10) to randomAlphaOfLengthBetween(1, 10))
        )

        val allocationActionString = allocationAction.toJsonString()
        val parsedAllocationAction = ISMActionsParser.instance.parse(parser(allocationActionString), 0)
        assertEquals("Round tripping AllocationAction doesn't work", allocationAction.convertToMap(), parsedAllocationAction.convertToMap())
    }

    fun `test managed index config parsing`() {
        val config = randomManagedIndexConfig()
        val configTwo = config.copy(changePolicy = null)
        var configThree = config.copy()

        val configString = config.toJsonString()
        val configTwoString = configTwo.toJsonString()
        val configThreeString = configThree.toJsonString()
        val parsedConfig =
            parserWithType(configString).parseWithType(config.id, config.seqNo, config.primaryTerm, ManagedIndexConfig.Companion::parse)
        val parsedConfigTwo =
            parserWithType(configTwoString).parseWithType(configTwo.id, configTwo.seqNo, configTwo.primaryTerm, ManagedIndexConfig.Companion::parse)
        configThree = configThree.copy(id = "some_doc_id", seqNo = 17, primaryTerm = 1)
        val parsedConfigThree =
            parserWithType(configThreeString).parseWithType(configThree.id, configThree.seqNo, configThree.primaryTerm, ManagedIndexConfig.Companion::parse)

        assertEquals("Round tripping ManagedIndexConfig doesn't work", config, parsedConfig)
        assertEquals("Round tripping ManagedIndexConfig doesn't work with null change policy", configTwo, parsedConfigTwo)
        assertEquals("Round tripping ManagedIndexConfig doesn't work with id and version", configThree, parsedConfigThree)
    }

    fun `test rollup action parsing`() {
        val rollupAction = randomRollupActionConfig()
        val rollupActionString = rollupAction.toJsonString()
        val parsedRollupAction = ISMActionsParser.instance.parse(parser(rollupActionString), 0) as RollupAction

        assertEquals("Round tripping RollupAction doesn't work", rollupAction.actionIndex, parsedRollupAction.actionIndex)
        assertEquals("Round tripping RollupAction doesn't work", rollupAction.ismRollup, parsedRollupAction.ismRollup)
    }

    fun `test close action parsing`() {
        val closeAction = randomCloseActionConfig()
        val closeActionString = closeAction.toJsonString()
        val parsedCloseAction = ISMActionsParser.instance.parse(parser(closeActionString), 0)

        assertEquals("Round tripping CloseAction doesn't work", closeAction.convertToMap(), parsedCloseAction.convertToMap())
    }

    fun `test open action parsing`() {
        val openAction = randomOpenActionConfig()
        val openActionString = openAction.toJsonString()
        val parsedOpenAction = ISMActionsParser.instance.parse(parser(openActionString), 0)

        assertEquals("Round tripping OpenAction doesn't work", openAction.convertToMap(), parsedOpenAction.convertToMap())
    }

    fun `test shrink action parsing`() {
        val shrinkAction = randomShrinkAction()
        val shrinkActionString = shrinkAction.toJsonString()
        val parsedShrinkAction = ISMActionsParser.instance.parse(parser(shrinkActionString), 0)

        assertEquals("Round tripping ShrinkAction doesn't work", shrinkAction.convertToMap(), parsedShrinkAction.convertToMap())
    }

    fun `test managed index metadata parsing`() {
        val metadata = ManagedIndexMetaData(
            index = randomAlphaOfLength(10),
            indexUuid = randomAlphaOfLength(10),
            policyID = randomAlphaOfLength(10),
            policySeqNo = randomNonNegativeLong(),
            policyPrimaryTerm = randomNonNegativeLong(),
            policyCompleted = null,
            rolledOver = null,
            indexCreationDate = null,
            transitionTo = randomAlphaOfLength(10),
            stateMetaData = null,
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = null,
            info = null
        )
        val metadataString = metadata.toJsonString()
        val parsedMetaData = ManagedIndexMetaData.parse(parser(metadataString))
        assertEquals("Round tripping ManagedIndexMetaData doesn't work", metadata, parsedMetaData)
    }

    fun `test change policy parsing`() {
        val changePolicy = randomChangePolicy()

        val changePolicyString = changePolicy.toJsonString()
        val parsedChangePolicy = ChangePolicy.parse(parser(changePolicyString))
        assertEquals("Round tripping ChangePolicy doesn't work", changePolicy, parsedChangePolicy)
    }

    fun `test channel parsing`() {
        val channel = randomChannel()

        val channelString = channel.toJsonString()
        val parsedChannel = Channel.parse(parser(channelString))
        assertEquals("Round tripping Channel doesn't work", channel, parsedChannel)
    }

    private fun parser(xc: String): XContentParser {
        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
        parser.nextToken()
        return parser
    }

    private fun parserWithType(xc: String): XContentParser {
        return XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    }
}
