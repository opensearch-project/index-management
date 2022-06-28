/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:Suppress("TooManyFunctions")
package org.opensearch.indexmanagement.snapshotmanagement

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.client.Client
import org.opensearch.common.Strings
import org.opensearch.common.time.DateFormatter
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.DATE_FORMAT_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.DATE_FORMAT_TIMEZONE_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_DOC_ID_SUFFIX
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_METADATA_ID_SUFFIX
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_TYPE
import org.opensearch.snapshots.SnapshotsService
import org.opensearch.jobscheduler.spi.schedule.Schedule
import org.opensearch.rest.RestStatus
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.snapshots.SnapshotMissingException
import org.opensearch.transport.RemoteTransportException
import java.time.Instant
import java.time.Instant.now
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale
import org.opensearch.common.time.DateFormatters

private val log = LogManager.getLogger("o.i.s.SnapshotManagementHelper")

fun smPolicyNameToDocId(policyName: String) = "$policyName$SM_DOC_ID_SUFFIX"
fun smDocIdToPolicyName(docId: String) = docId.substringBeforeLast(SM_DOC_ID_SUFFIX)
fun smPolicyNameToMetadataDocId(policyName: String) = "$policyName$SM_METADATA_ID_SUFFIX"
fun smMetadataDocIdToPolicyName(docId: String) = docId.substringBeforeLast(SM_METADATA_ID_SUFFIX)

@Suppress("RethrowCaughtException", "ThrowsCount")
suspend fun Client.getSMPolicy(policyID: String): SMPolicy {
    try {
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, policyID)
        val getResponse: GetResponse = this.suspendUntil { get(getRequest, it) }
        if (!getResponse.isExists || getResponse.isSourceEmpty) {
            throw OpenSearchStatusException("Snapshot management policy not found", RestStatus.NOT_FOUND)
        }
        return parseSMPolicy(getResponse)
    } catch (e: OpenSearchStatusException) {
        throw e
    } catch (e: IndexNotFoundException) {
        throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
    } catch (e: IllegalArgumentException) {
        log.error("Failed to retrieve snapshot management policy [$policyID]", e)
        throw OpenSearchStatusException("Snapshot management policy could not be parsed", RestStatus.INTERNAL_SERVER_ERROR)
    } catch (e: Exception) {
        log.error("Failed to retrieve snapshot management policy [$policyID]", e)
        throw OpenSearchStatusException("Failed to retrieve Snapshot management policy.", RestStatus.NOT_FOUND)
    }
}

@Suppress("RethrowCaughtException", "ThrowsCount")
suspend fun Client.getSMMetadata(jobID: String): SMMetadata? {
    val metadataID = smPolicyNameToMetadataDocId(smDocIdToPolicyName(jobID))
    try {
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, metadataID).routing(jobID)
        val getResponse: GetResponse = this.suspendUntil { get(getRequest, it) }
        if (!getResponse.isExists || getResponse.isSourceEmpty) {
            return null
        }
        return parseSMMetadata(getResponse)
    } catch (e: OpenSearchStatusException) {
        throw e
    } catch (e: IndexNotFoundException) {
        log.error("Failed to retrieve snapshot management metadata [$metadataID] because config index did not exist", e)
        throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
    } catch (e: IllegalArgumentException) {
        log.error("Failed to retrieve snapshot management metadata [$metadataID]", e)
        throw OpenSearchStatusException("Snapshot management metadata could not be parsed", RestStatus.INTERNAL_SERVER_ERROR)
    } catch (e: Exception) {
        log.error("Failed to retrieve snapshot management metadata [$metadataID]", e)
        throw OpenSearchStatusException("Failed to retrieve Snapshot management metadata.", RestStatus.NOT_FOUND)
    }
}

fun parseSMPolicy(response: GetResponse, xContentRegistry: NamedXContentRegistry = NamedXContentRegistry.EMPTY): SMPolicy {
    val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.sourceAsBytesRef, XContentType.JSON)
    return xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, SMPolicy.Companion::parse)
}

fun parseSMMetadata(response: GetResponse, xContentRegistry: NamedXContentRegistry = NamedXContentRegistry.EMPTY): SMMetadata {
    val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.sourceAsBytesRef, XContentType.JSON)
    return xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, SMMetadata.Companion::parse)
}

/**
 * Save snapshot management job run metadata
 *
 * @param id: snapshot management job doc id
 */
suspend fun Client.indexMetadata(
    metadata: SMMetadata,
    id: String,
    seqNo: Long,
    primaryTerm: Long,
    create: Boolean = false,
): IndexResponse {
    val indexReq = IndexRequest(INDEX_MANAGEMENT_INDEX).create(create)
        .id(smPolicyNameToMetadataDocId(smDocIdToPolicyName(id)))
        .source(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
        .setIfSeqNo(seqNo)
        .setIfPrimaryTerm(primaryTerm)
        .routing(id)

    return suspendUntil { index(indexReq, it) }
}

fun generateSnapshotName(policy: SMPolicy): String {
    var result: String = policy.policyName
    var dateFormat = policy.snapshotConfig[DATE_FORMAT_FIELD] as String?
    if (dateFormat == null) {
        dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
    }
    val dateValue = if (policy.snapshotConfig[DATE_FORMAT_TIMEZONE_FIELD] != null) {
        generateFormatDate(
            dateFormat,
            ZoneId.of(policy.snapshotConfig[DATE_FORMAT_TIMEZONE_FIELD] as String),
        )
    } else {
        generateFormatDate(dateFormat)
    }.lowercase()
    result += "-$dateValue"
    return result + "-${getRandomString(RANDOM_STRING_LENGTH)}"
}

const val RANDOM_STRING_LENGTH = 8

fun getRandomString(length: Int): String {
    val allowedChars = ('a'..'z') + ('0'..'9')
    return List(length) { allowedChars.random() }
        .joinToString("")
}

/**
 * For the supporting formats, refer to [DateFormatters]
 */
fun generateFormatDate(dateFormat: String, timezone: ZoneId = ZoneId.of("UTC")): String {
    val dateFormatter = DateFormatter.forPattern(dateFormat).withZone(timezone)
    return dateFormatter.format(now())
}

fun validateDateFormat(dateFormat: String): String? {
    return try {
        val timeZone = ZoneId.systemDefault()
        val dateFormatter = DateFormatter.forPattern(dateFormat).withZone(timeZone)
        val instant = dateFormatter.toDateMathParser().parse("now/s", System::currentTimeMillis, false, timeZone)
        dateFormatter.format(instant)
        null
    } catch (e: Exception) {
        e.message ?: "Invalid date format."
    }
}

fun preFixTimeStamp(msg: String?): String {
    val formatter = DateTimeFormatter.ISO_INSTANT
    return "[${formatter.format(now().truncatedTo(ChronoUnit.SECONDS))}]: " + msg
}

fun addSMPolicyInSnapshotMetadata(snapshotConfig: Map<String, Any>, policyName: String): Map<String, Any> {
    var snapshotMetadata = snapshotConfig["metadata"] as MutableMap<String, String>?
    if (snapshotMetadata != null) {
        snapshotMetadata[SM_TYPE] = policyName
    } else {
        snapshotMetadata = mutableMapOf(SM_TYPE to policyName)
    }
    val snapshotConfigWithSMPolicyMetadata = snapshotConfig.toMutableMap()
    snapshotConfigWithSMPolicyMetadata["metadata"] = snapshotMetadata
    return snapshotConfigWithSMPolicyMetadata
}

fun List<SnapshotInfo>.filterBySMPolicyInSnapshotMetadata(policyName: String): List<SnapshotInfo> {
    return filter { it.userMetadata()?.get(SM_TYPE) == policyName }
}

/**
 * Get snapshots
 *
 * @param name: exact snapshot name or partial name with * at the end
 * @return list of snapshot management snapshot info sorted by start time
 */
suspend fun Client.getSnapshots(name: String, repo: String): List<SnapshotInfo> {
    try {
        val req = GetSnapshotsRequest()
            .snapshots(arrayOf(name))
            .repository(repo)
        val res: GetSnapshotsResponse = admin().cluster().suspendUntil { getSnapshots(req, it) }
        return res.snapshots
    } catch (ex: RemoteTransportException) {
        val unwrappedException = ExceptionsHelper.unwrapCause(ex) as Exception
        throw unwrappedException
    }
}

/**
 * Used in Snapshot Management States logic
 */
@Suppress("LongParameterList", "ReturnCount")
suspend fun Client.getSnapshots(
    job: SMPolicy,
    name: String,
    metadataBuilder: SMMetadata.Builder,
    log: Logger,
    snapshotMissingMsg: String?,
    exceptionMsg: String,
): GetSnapshotsResult {
    val snapshots = try {
        getSnapshots(
            name,
            job.snapshotConfig["repository"] as String
        )
    } catch (ex: SnapshotMissingException) {
        snapshotMissingMsg?.let { log.warn(snapshotMissingMsg) }
        return GetSnapshotsResult(false, emptyList(), metadataBuilder)
    } catch (ex: Exception) {
        log.error(exceptionMsg, ex)
        metadataBuilder.setLatestExecution(
            status = SMMetadata.LatestExecution.Status.RETRYING,
            message = exceptionMsg,
            cause = ex,
        )
        return GetSnapshotsResult(true, emptyList(), metadataBuilder)
    }.filterBySMPolicyInSnapshotMetadata(job.policyName)

    return GetSnapshotsResult(false, snapshots, metadataBuilder)
}

data class GetSnapshotsResult(
    val failed: Boolean,
    val snapshots: List<SnapshotInfo>,
    val metadataBuilder: SMMetadata.Builder,
)

fun tryUpdatingNextExecutionTime(
    metadataBuilder: SMMetadata.Builder,
    nextTime: Instant,
    schedule: Schedule,
    workflowType: WorkflowType,
    log: Logger
): UpdateNextExecutionTimeResult {
    val now = now()
    return if (!now.isBefore(nextTime)) {
        log.info("Current time [${now()}] has passed nextExecutionTime [$nextTime].")
        val newNextTime = schedule.getNextExecutionTime(now)
        when (workflowType) {
            WorkflowType.CREATION -> {
                metadataBuilder.setNextCreationTime(newNextTime)
            }
            WorkflowType.DELETION -> {
                metadataBuilder.setNextDeletionTime(newNextTime)
            }
        }
        UpdateNextExecutionTimeResult(true, metadataBuilder)
    } else {
        log.debug("Current time [${now()}] has not passed nextExecutionTime [$nextTime]")
        // TODO SM dynamically update job start_time to avoid unnecessary job runs
        UpdateNextExecutionTimeResult(false, metadataBuilder)
    }
}

data class UpdateNextExecutionTimeResult(
    val updated: Boolean,
    val metadataBuilder: SMMetadata.Builder,
)

/**
 * Snapshot management policy will be used as the prefix to create snapshot,
 * so it conforms to snapshot name format validated in [SnapshotsService]
 */
fun validateSMPolicyName(policyName: String) {
    val errorMessages: MutableList<String> = mutableListOf()
    if (policyName.isEmpty()) {
        errorMessages.add("Policy name cannot be empty.")
    }
    if (policyName.contains(" ")) {
        errorMessages.add("Policy name must not contain whitespace.")
    }
    if (policyName.contains(",")) {
        errorMessages.add("Policy name must not contain ','.")
    }
    if (policyName.contains("#")) {
        errorMessages.add("Policy name must not contain '#'.")
    }
    if (policyName.startsWith("_")) {
        errorMessages.add("Policy name must not start with '_'.")
    }
    if (policyName.lowercase(Locale.ROOT) != policyName) {
        errorMessages.add("Policy name must be lowercase.")
    }
    if (!Strings.validFileName(policyName)) {
        errorMessages.add(
            "Policy name must not contain the following characters " + Strings.INVALID_FILENAME_CHARS + "."
        )
    }
    if (errorMessages.isNotEmpty()) {
        throw IllegalArgumentException(errorMessages.joinToString(separator = "\n"))
    }
}

fun TimeValue.isExceed(startTime: Instant?): Boolean {
    startTime ?: return false
    return (now().toEpochMilli() - startTime.toEpochMilli()) > this.millis
}

fun timeLimitExceeded(
    timeLimit: TimeValue,
    metadataBuilder: SMMetadata.Builder,
    workflow: WorkflowType,
    log: Logger,
): SMResult {
    val message = getTimeLimitExceededMessage(timeLimit, workflow)
    log.warn(message)
    metadataBuilder.setLatestExecution(
        status = SMMetadata.LatestExecution.Status.TIME_LIMIT_EXCEEDED,
        cause = SnapshotManagementException(message = message),
        endTime = now(),
    )
    return SMResult.Fail(metadataBuilder, workflow, forceReset = true)
}

fun getTimeLimitExceededMessage(timeLimit: TimeValue, workflow: WorkflowType): String {
    val workflow = when (workflow) {
        WorkflowType.CREATION -> {
            "creation"
        }
        WorkflowType.DELETION -> {
            "deletion"
        }
    }
    return "Time limit $timeLimit exceeded during snapshot $workflow step"
}
