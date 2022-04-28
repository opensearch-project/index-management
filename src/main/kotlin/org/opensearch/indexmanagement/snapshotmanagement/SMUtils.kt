/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.client.Client
import org.opensearch.common.time.DateFormatter
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.jobscheduler.spi.schedule.Schedule
import java.time.Instant
import java.time.ZoneId

private val log = LogManager.getLogger("Snapshot Management Helper")

fun getSMDocId(policyName: String) = "$policyName-sm"
fun getSMMetadataDocId(policyName: String) = "$policyName-sm-metadata"

suspend fun Client.indexMetadata(
    metadata: SMMetadata,
    policyName: String,
    seqNo: Long = metadata.seqNo,
    primaryTerm: Long = metadata.primaryTerm,
    create: Boolean = false
): IndexResponse {
    val indexReq = IndexRequest(INDEX_MANAGEMENT_INDEX).create(create)
        .id(getSMMetadataDocId(policyName))
        .source(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
        .setIfSeqNo(seqNo)
        .setIfPrimaryTerm(primaryTerm)
        .routing(policyName)

    return suspendUntil { index(indexReq, it) }
}

suspend fun Client.getMetadata(policyName: String): SMMetadata? {
    val getReq = GetRequest(INDEX_MANAGEMENT_INDEX, getSMMetadataDocId(policyName)).routing(policyName)
    val getRes: GetResponse = suspendUntil { get(getReq, it) }
    var metadata: SMMetadata? = null
    if (getRes.isExists) {
        // log.info("Get metadata response: ${getRes.sourceAsBytesRef.utf8ToString()}")
        val xcp = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            getRes.sourceAsBytesRef, XContentType.JSON
        )
        metadata = xcp.parseWithType(getRes.id, getRes.seqNo, getRes.primaryTerm, SMMetadata.Companion::parse)
        log.info("Parse metadata: $metadata")
    }
    return metadata
}

fun getNextExecutionTime(schedule: Schedule, fromTime: Instant): Instant {
    return when (schedule) {
        is CronSchedule -> {
            log.info("next execution time: ${schedule.getNextExecutionTime(fromTime)}")
            log.info("duration until next execution: ${schedule.nextTimeToExecute()}")
            schedule.getNextExecutionTime(fromTime)
        }
        is IntervalSchedule -> {
            // TODO: based on last success snapshot start time or end time
            log.info("next execution time: ${schedule.getNextExecutionTime(fromTime)}.")
            log.info("duration until next execution: ${schedule.nextTimeToExecute()}.")
            schedule.getNextExecutionTime(fromTime)
        }
        else -> throw IllegalArgumentException("Schedule type is not in [CronSchedule, IntervalSchedule].")
    }
}

fun generateSnapshotName(policy: SMPolicy): String {
    var result: String = policy.policyName
    if (policy.snapshotConfig["date_format"] != null) {
        val dateFormat = generateFormatTime(policy.snapshotConfig["date_format"] as String)
        result += "-$dateFormat"
    }
    // TODO add hash suffix
    return result
}

fun generateFormatTime(dateFormat: String): String {
    val timeZone = ZoneId.of("America/Los_Angeles")
    val dateFormatter = DateFormatter.forPattern(dateFormat).withZone(timeZone)
    val instant = dateFormatter.toDateMathParser().parse("now/s", System::currentTimeMillis, false, timeZone)
    return dateFormatter.format(instant)
}

suspend fun SMStateMachine.startTransaction() {
    metadataToSave = metadata.copy(apiCalling = true)
    updateMetadata()
    log.info("Start transaction by setting api_calling to true.")
}

suspend fun SMStateMachine.revertTransaction() {
    metadataToSave = metadata.copy(apiCalling = false)
    updateMetadata()
    log.info("Set api_calling to false.")
}
