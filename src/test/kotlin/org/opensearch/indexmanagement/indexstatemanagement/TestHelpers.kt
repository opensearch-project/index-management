/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.model.Conditions
import org.opensearch.indexmanagement.indexstatemanagement.model.ErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.StateFilter
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.AllocationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.DeleteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ForceMergeActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.NotificationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ReadOnlyActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ReadWriteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ReplicaCountActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.IndexPriorityActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.RolloverActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.RollupActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.SnapshotActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.coordinator.ClusterStateManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Chime
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.CustomWebhook
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Destination
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.DestinationType
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Slack
import org.opensearch.indexmanagement.rollup.randomISMRollup
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.jobscheduler.spi.schedule.Schedule
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit

fun randomPolicy(
    id: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    description: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    schemaVersion: Long = OpenSearchRestTestCase.randomLong(),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    errorNotification: ErrorNotification? = randomErrorNotification(),
    states: List<State> = List(OpenSearchRestTestCase.randomIntBetween(1, 10)) { randomState() },
    ismTemplate: ISMTemplate? = null
): Policy {
    return Policy(id = id, schemaVersion = schemaVersion, lastUpdatedTime = lastUpdatedTime,
            errorNotification = errorNotification, defaultState = states[0].name, states = states, description = description, ismTemplate = ismTemplate)
}

fun randomState(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    actions: List<ActionConfig> = listOf(),
    transitions: List<Transition> = listOf()
): State {
    return State(name = name, actions = actions, transitions = transitions)
}

fun randomTransition(
    stateName: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    conditions: Conditions? = randomConditions()
): Transition {
    return Transition(stateName = stateName, conditions = conditions)
}

/**
 * TODO: Excluded randomCronSchedule being included in randomConditions as two issues need to be resolved first:
 *   1. Job Scheduler needs to be published to maven central as there is an issue retrieving dependencies for SPI
 *   2. CronSchedule in Job Scheduler needs to implement equals/hash methods so assertEquals compares two CronSchedule
 *      objects properly when doing roundtrip parsing tests
 */
fun randomConditions(
    condition: Pair<String, Any>? =
        OpenSearchRestTestCase.randomFrom(listOf(randomIndexAge(), randomDocCount(), randomSize(), null))
): Conditions? {

    if (condition == null) return null

    val type = condition.first
    val value = condition.second

    return when (type) {
        Conditions.MIN_INDEX_AGE_FIELD -> Conditions(indexAge = value as TimeValue)
        Conditions.MIN_DOC_COUNT_FIELD -> Conditions(docCount = value as Long)
        Conditions.MIN_SIZE_FIELD -> Conditions(size = value as ByteSizeValue)
//        Conditions.CRON_FIELD -> Conditions(cron = value as CronSchedule) // TODO: Uncomment after issues are fixed
        else -> throw IllegalArgumentException("Invalid field: [$type] given for random Conditions.")
    }
}

fun nonNullRandomConditions(): Conditions =
    randomConditions(OpenSearchRestTestCase.randomFrom(listOf(randomIndexAge(), randomDocCount(), randomSize())))!!

fun randomDeleteActionConfig(): DeleteActionConfig {
    return DeleteActionConfig(index = 0)
}

fun randomRolloverActionConfig(
    minSize: ByteSizeValue = randomByteSizeValue(),
    minDocs: Long = OpenSearchRestTestCase.randomLongBetween(1, 1000),
    minAge: TimeValue = randomTimeValueObject()
): RolloverActionConfig {
    return RolloverActionConfig(
        minSize = minSize,
        minDocs = minDocs,
        minAge = minAge,
        index = 0
    )
}

fun randomReadOnlyActionConfig(): ReadOnlyActionConfig {
    return ReadOnlyActionConfig(index = 0)
}

fun randomReadWriteActionConfig(): ReadWriteActionConfig {
    return ReadWriteActionConfig(index = 0)
}

fun randomReplicaCountActionConfig(numOfReplicas: Int = OpenSearchRestTestCase.randomIntBetween(0, 200)): ReplicaCountActionConfig {
    return ReplicaCountActionConfig(index = 0, numOfReplicas = numOfReplicas)
}

fun randomIndexPriorityActionConfig(indexPriority: Int = OpenSearchRestTestCase.randomIntBetween(0, 100)): IndexPriorityActionConfig {
    return IndexPriorityActionConfig(index = 0, indexPriority = indexPriority)
}

fun randomForceMergeActionConfig(
    maxNumSegments: Int = OpenSearchRestTestCase.randomIntBetween(1, 50)
): ForceMergeActionConfig {
    return ForceMergeActionConfig(maxNumSegments = maxNumSegments, index = 0)
}

fun randomNotificationActionConfig(
    destination: Destination = randomDestination(),
    messageTemplate: Script = randomTemplateScript("random message"),
    index: Int = 0
): NotificationActionConfig {
    return NotificationActionConfig(destination, messageTemplate, index)
}

fun randomAllocationActionConfig(require: Map<String, String> = emptyMap(), exclude: Map<String, String> = emptyMap(), include: Map<String, String> = emptyMap()): AllocationActionConfig {
    return AllocationActionConfig(require, include, exclude, index = 0)
}

fun randomRollupActionConfig(): RollupActionConfig {
    return RollupActionConfig(ismRollup = randomISMRollup(), index = 0)
}

fun randomDestination(type: DestinationType = randomDestinationType()): Destination {
    return Destination(
        type = type,
        chime = if (type == DestinationType.CHIME) randomChime() else null,
        slack = if (type == DestinationType.SLACK) randomSlack() else null,
        customWebhook = if (type == DestinationType.CUSTOM_WEBHOOK) randomCustomWebhook() else null
    )
}

fun randomDestinationType(): DestinationType {
    val types = listOf(DestinationType.SLACK, DestinationType.CHIME, DestinationType.CUSTOM_WEBHOOK)
    return OpenSearchRestTestCase.randomSubsetOf(1, types).first()
}

fun randomChime(): Chime {
    return Chime("https://www.amazon.com")
}

fun randomSlack(): Slack {
    return Slack("https://www.amazon.com")
}

fun randomCustomWebhook(): CustomWebhook {
    return CustomWebhook(
        url = "https://www.amazon.com",
        scheme = null,
        host = null,
        port = -1,
        path = null,
        queryParams = emptyMap(),
        headerParams = emptyMap(),
        username = null,
        password = null
    )
}

fun randomTemplateScript(
    source: String,
    params: Map<String, String> = emptyMap()
): Script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, source, params)

fun randomSnapshotActionConfig(repository: String = "repo", snapshot: String = "sp"): SnapshotActionConfig {
    return SnapshotActionConfig(repository, snapshot, index = 0)
}

/**
 * Helper functions for creating a random Conditions object
 */
fun randomIndexAge(indexAge: TimeValue = randomTimeValueObject()) = Conditions.MIN_INDEX_AGE_FIELD to indexAge

fun randomDocCount(docCount: Long = OpenSearchRestTestCase.randomLongBetween(1, 1000)) = Conditions.MIN_DOC_COUNT_FIELD to docCount

fun randomSize(size: ByteSizeValue = randomByteSizeValue()) = Conditions.MIN_SIZE_FIELD to size

fun randomCronSchedule(cron: CronSchedule = CronSchedule("0 * * * *", ZoneId.of("UTC"))) =
    Conditions.CRON_FIELD to cron

fun randomTimeValueObject(): TimeValue = TimeValue.parseTimeValue(OpenSearchRestTestCase.randomPositiveTimeValue(), "")

fun randomByteSizeValue(): ByteSizeValue =
    ByteSizeValue.parseBytesSizeValue(
        OpenSearchRestTestCase.randomIntBetween(1, 1000).toString() + OpenSearchRestTestCase.randomFrom(listOf("b", "kb", "mb", "gb")),
        ""
    )
/**
 * End - Conditions helper functions
 */

fun randomChangePolicy(
    policyID: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    state: String? = if (OpenSearchRestTestCase.randomBoolean()) OpenSearchRestTestCase.randomAlphaOfLength(10) else null,
    include: List<StateFilter> = emptyList(),
    isSafe: Boolean = false
): ChangePolicy {
    return ChangePolicy(policyID, state, include, isSafe)
}

// will only return null since we dont want to send actual notifications during integ tests
fun randomErrorNotification(): ErrorNotification? = null

fun randomManagedIndexConfig(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    index: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    uuid: String = OpenSearchRestTestCase.randomAlphaOfLength(20),
    enabled: Boolean = OpenSearchRestTestCase.randomBoolean(),
    schedule: Schedule = IntervalSchedule(Instant.ofEpochMilli(Instant.now().toEpochMilli()), 5, ChronoUnit.MINUTES),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    policyID: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    policy: Policy? = randomPolicy(),
    changePolicy: ChangePolicy? = randomChangePolicy()
): ManagedIndexConfig {
    return ManagedIndexConfig(
        jobName = name,
        index = index,
        indexUuid = uuid,
        enabled = enabled,
        jobSchedule = schedule,
        jobLastUpdatedTime = lastUpdatedTime,
        jobEnabledTime = enabledTime,
        policyID = policy?.id ?: policyID,
        policySeqNo = policy?.seqNo,
        policyPrimaryTerm = policy?.primaryTerm,
        policy = policy?.copy(id = ManagedIndexConfig.NO_ID, seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM),
        changePolicy = changePolicy
    )
}

fun randomClusterStateManagedIndexConfig(
    index: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    uuid: String = OpenSearchRestTestCase.randomAlphaOfLength(20),
    policyID: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
): ClusterStateManagedIndexConfig {
    return ClusterStateManagedIndexConfig(
        index = index,
        uuid = uuid,
        policyID = policyID,
        seqNo = seqNo,
        primaryTerm = primaryTerm
    )
}

fun randomSweptManagedIndexConfig(
    index: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    uuid: String = OpenSearchRestTestCase.randomAlphaOfLength(20),
    policyID: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    changePolicy: ChangePolicy? = null,
    policy: Policy? = null
): SweptManagedIndexConfig {
    return SweptManagedIndexConfig(
        index = index,
        uuid = uuid,
        policyID = policyID,
        seqNo = seqNo,
        primaryTerm = primaryTerm,
        policy = policy,
        changePolicy = changePolicy
    )
}

fun randomISMTemplate(
    indexPatterns: List<String> = listOf(OpenSearchRestTestCase.randomAlphaOfLength(10) + "*"),
    priority: Int = OpenSearchRestTestCase.randomIntBetween(0, 100),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)
): ISMTemplate {
    return ISMTemplate(
        indexPatterns = indexPatterns,
        priority = priority,
        lastUpdatedTime = lastUpdatedTime
    )
}

fun Policy.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder).string()
}

fun State.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun Transition.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun Conditions.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun DeleteActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun RolloverActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ReadOnlyActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ReadWriteActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ReplicaCountActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun IndexPriorityActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ForceMergeActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun NotificationActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun AllocationActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ChangePolicy.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ManagedIndexConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ManagedIndexMetaData.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder().startObject()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject().string()
}

fun SnapshotActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun RollupActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ISMTemplate.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun <T> wait(
    timeout: Instant = Instant.ofEpochSecond(10),
    block: () -> T
) {
    val startTime = Instant.now().toEpochMilli()
    do {
        try {
            block()
            if ((Instant.now().toEpochMilli() - startTime) > timeout.toEpochMilli()) {
                return
            } else {
                Thread.sleep(100L)
            }
        } catch (e: Throwable) {
            throw e
        }
    } while (true)
}
