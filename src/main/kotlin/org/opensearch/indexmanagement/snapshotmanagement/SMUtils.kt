/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
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
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.jobscheduler.spi.schedule.Schedule
import org.opensearch.snapshots.SnapshotInfo
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

private val log = LogManager.getLogger("o.o.i.s.SnapshotManagementHelper")

const val smSuffix = "-sm"
const val smMetadataSuffix = "-metadata"
fun smPolicyNameToJobId(policyName: String) = "$policyName$smSuffix"
fun smJobIdToPolicyName(id: String) = id.substringBeforeLast(smSuffix)
fun smMetadataId(jobId: String) = "$jobId$smMetadataSuffix"

/**
 * Save snapshot management job run metadata
 *
 * @param id: snapshot management job doc id
 */
suspend fun Client.indexMetadata(
    metadata: SMMetadata,
    id: String,
    seqNo: Long = metadata.seqNo,
    primaryTerm: Long = metadata.primaryTerm,
    create: Boolean = false
): IndexResponse {
    val indexReq = IndexRequest(INDEX_MANAGEMENT_INDEX).create(create)
        .id(smMetadataId(id))
        .source(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
        .setIfSeqNo(seqNo)
        .setIfPrimaryTerm(primaryTerm)
        .routing(id)

    return suspendUntil { index(indexReq, it) }
}

/**
 * Retrieve snapshot management job run metadata
 *
 * @return null indicate the retrieved metadata doesn't exist
 */
suspend fun Client.getMetadata(job: SMPolicy): SMMetadata? {
    val getReq = GetRequest(INDEX_MANAGEMENT_INDEX, smMetadataId(job.id)).routing(job.id)
    val getRes: GetResponse = suspendUntil { get(getReq, it) }
    if (getRes.isExists) {
        log.info("sm dev: Get metadata response: ${getRes.sourceAsBytesRef.utf8ToString()}")
        val xcp = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            getRes.sourceAsBytesRef, XContentType.JSON
        )
        val metadata = xcp.parseWithType(getRes.id, getRes.seqNo, getRes.primaryTerm, SMMetadata.Companion::parse)
        log.info("sm dev: Parse metadata: $metadata")
        return metadata
    }
    return null
}

fun getNextExecutionTime(schedule: Schedule, fromTime: Instant): Instant {
    return when (schedule) {
        is CronSchedule -> {
            log.info("sm dev: next execution time: ${schedule.getNextExecutionTime(fromTime)}")
            log.info("sm dev: duration until next execution: ${schedule.nextTimeToExecute()}")
            schedule.getNextExecutionTime(fromTime)
        }
        is IntervalSchedule -> {
            TODO("Not yet implemented")
        }
        else -> throw IllegalArgumentException("Schedule type is not in [CronSchedule, IntervalSchedule].")
    }
}

fun generateSnapshotName(policy: SMPolicy): String {
    var result: String = smJobIdToPolicyName(policy.id)
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

// suspend fun SMStateMachine.startCreateSnapshotTransaction() {
//     updateMetadata(metadata.copy(
//         creation = metadata.creation.copy(
//             atomic = true
//         )
//     ))
//     log.info("Started create snapshot transaction.")
// }
//
// suspend fun SMStateMachine.startDeleteSnapshotTransaction() {
//     updateMetadata(metadata.copy(
//         deletion = metadata.deletion.copy(
//             atomic = true
//         )
//     ))
//     log.info("Started delete snapshot transaction.")
// }
//
// suspend fun SMStateMachine.endCreateSnapshotTransaction() {
//     updateMetadata(metadata.copy(
//         creation = metadata.creation.copy(
//             atomic = false
//         )
//     ))
//     log.info("End create snapshot transaction.")
// }
//
// suspend fun SMStateMachine.endDeleteSnapshotTransaction() {
//     updateMetadata(metadata.copy(
//         deletion = metadata.deletion.copy(
//             atomic = false
//         )
//     ))
//     log.info("End delete snapshot transaction.")
// }

fun preFixTimeStamp(msg: String?): String {
    val formatter = DateTimeFormatter.ISO_INSTANT
    return "[${formatter.format(Instant.now().truncatedTo(ChronoUnit.SECONDS))}]: " + msg
}

/**
 * Get snapshots
 *
 * @param name: exact snapshot name or partial name with * at the end
 * @return list of snapshot management snapshot info sorted by start time
 */
suspend fun Client.getSnapshots(name: String, repo: String): List<SnapshotInfo> {
    val req = GetSnapshotsRequest()
        .snapshots(arrayOf(name))
        .repository(repo)
    val res: GetSnapshotsResponse = admin().cluster().suspendUntil { getSnapshots(req, it) }
    log.info("Get snapshot response: ${res.snapshots}")
    return res.snapshots
}
