/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
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
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.DATE_FORMAT_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_DOC_ID_SUFFIX
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_METADATA_ID_SUFFIX
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_TYPE
import org.opensearch.snapshots.SnapshotsService
import org.opensearch.jobscheduler.spi.schedule.Schedule
import org.opensearch.rest.RestStatus
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.snapshots.SnapshotMissingException
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale

private val log = LogManager.getLogger("o.i.s.SnapshotManagementHelper")

fun smPolicyNameToDocId(policyName: String) = "$policyName$SM_DOC_ID_SUFFIX"
fun smDocIdToPolicyName(id: String) = id.substringBeforeLast(SM_DOC_ID_SUFFIX)
fun smPolicyNameToMetadataId(policyName: String) = "$policyName$SM_METADATA_ID_SUFFIX"
fun smMetadataIdToPolicyName(id: String) = id.substringBeforeLast(SM_METADATA_ID_SUFFIX)

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
suspend fun Client.getSMMetadata(jobID: String): SMMetadata {
    val metadataID = smPolicyNameToMetadataId(smDocIdToPolicyName(jobID))
    try {
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, metadataID).routing(jobID)
        val getResponse: GetResponse = this.suspendUntil { get(getRequest, it) }
        if (!getResponse.isExists || getResponse.isSourceEmpty) {
            throw OpenSearchStatusException("Snapshot management metadata not found", RestStatus.NOT_FOUND)
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
    create: Boolean = false
): IndexResponse {
    val indexReq = IndexRequest(INDEX_MANAGEMENT_INDEX).create(create)
        .id(smPolicyNameToMetadataId(smDocIdToPolicyName(id)))
        .source(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
        .setIfSeqNo(metadata.seqNo)
        .setIfPrimaryTerm(metadata.primaryTerm)
        .routing(id)

    return suspendUntil { index(indexReq, it) }
}

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
        .id(smPolicyNameToMetadataId(smDocIdToPolicyName(id)))
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
    val getReq = GetRequest(INDEX_MANAGEMENT_INDEX, smPolicyNameToMetadataId(smDocIdToPolicyName(job.id))).routing(job.id)
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

fun generateSnapshotName(policy: SMPolicy): String {
    var result: String = smDocIdToPolicyName(policy.id)
    if (policy.snapshotConfig[DATE_FORMAT_FIELD] != null) {
        val dateFormat = generateFormatTime(policy.snapshotConfig[DATE_FORMAT_FIELD] as String)
        result += "-$dateFormat"
    }
    return result + "-${getRandomString(8)}"
}

fun getRandomString(length: Int): String {
    val allowedChars = ('a'..'z') + ('0'..'9')
    return (1..length)
        .map { allowedChars.random() }
        .joinToString("")
}

fun generateFormatTime(dateFormat: String): String {
    return try {
        val timeZone = ZoneId.systemDefault()
        val dateFormatter = DateFormatter.forPattern(dateFormat).withZone(timeZone)
        val instant = dateFormatter.toDateMathParser().parse("now/s", System::currentTimeMillis, false, timeZone)
        dateFormatter.format(instant)
    } catch (e: Exception) {
        "invalid_date_format"
    }
}

fun preFixTimeStamp(msg: String?): String {
    val formatter = DateTimeFormatter.ISO_INSTANT
    return "[${formatter.format(Instant.now().truncatedTo(ChronoUnit.SECONDS))}]: " + msg
}

fun addSMPolicyInSnapshotMetadata(snapshotConfig: Map<String, Any>, policyName: String): Map<String, Any> {
    var snapshotMetadata = snapshotConfig["metadata"]
    if (snapshotMetadata != null) {
        snapshotMetadata as Map<String, String>
        snapshotMetadata = snapshotMetadata.toMutableMap()
        snapshotMetadata[SM_TYPE] = policyName
        log.info("sm dev metadata with sm policy $snapshotMetadata")
    } else {
        snapshotMetadata = mapOf(SM_TYPE to policyName)
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
    val req = GetSnapshotsRequest()
        .snapshots(arrayOf(name))
        .repository(repo)
    val res: GetSnapshotsResponse = admin().cluster().suspendUntil { getSnapshots(req, it) }
    log.info("sm dev: Get snapshot response: ${res.snapshots.map { it.snapshotId().name }}")
    return res.snapshots
}

/**
 * Used in Snapshot Management States logic
 */
suspend fun Client.getSnapshotsWithErrorHandling(
    job: SMPolicy,
    name: String,
    metadataBuilder: SMMetadata.Builder,
    log: Logger,
    snapshotMissingMsg: String?,
    exceptionMsg: String,
): GetSnapshotsResult {
    val snapshots = try {
        log.info("sm dev get snapshot of $name")
        getSnapshots(
            name,
            job.snapshotConfig["repository"] as String
        )
    } catch (ex: SnapshotMissingException) {
        log.info("sm dev get snapshot missing exception")
        snapshotMissingMsg?.let { log.warn(snapshotMissingMsg) }
        emptyList()
    } catch (ex: Exception) {
        log.error(exceptionMsg, ex)
        metadataBuilder.setLatestExecution(
            status = SMMetadata.LatestExecution.Status.RETRYING,
            message = exceptionMsg,
            cause = SnapshotManagementException.wrap(ex).message
        )
        return GetSnapshotsResult(emptyList(), metadataBuilder, true)
    }.filterBySMPolicyInSnapshotMetadata(job.policyName)

    return GetSnapshotsResult(snapshots, metadataBuilder, false)
}

data class GetSnapshotsResult(
    val snapshots: List<SnapshotInfo>,
    val metadataBuilder: SMMetadata.Builder,
    val failed: Boolean,
)

fun updateNextExecutionTime(
    metadataBuilder: SMMetadata.Builder,
    nextTime: Instant,
    schedule: Schedule,
    workflowType: WorkflowType,
    log: Logger
): UpdateNextExecutionTimeResult {
    val now = Instant.now()
    return if (!now.isBefore(nextTime)) {
        log.info("sm dev: Current time [${Instant.now()}] has passed nextExecutionTime [$nextTime]")
        when (workflowType) {
            WorkflowType.CREATION -> {
                metadataBuilder.setNextCreationTime(schedule.getNextExecutionTime(now))
            }
            WorkflowType.DELETION -> {
                metadataBuilder.setNextDeletionTime(schedule.getNextExecutionTime(now))
            }
        }
        UpdateNextExecutionTimeResult(true, metadataBuilder)
    } else {
        log.info("sm dev: Current time [${Instant.now()}] has not passed nextExecutionTime [$nextTime]")
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
    if (policyName.isBlank()) {
        throw SnapshotManagementException(message = "Policy name cannot be empty")
    }
    if (policyName.contains(" ")) {
        throw SnapshotManagementException(message = "Policy name must not contain whitespace")
    }
    if (policyName.contains(",")) {
        throw SnapshotManagementException(message = "Policy name must not contain ','")
    }
    if (policyName.contains("#")) {
        throw SnapshotManagementException(message = "Policy name must not contain '#'")
    }
    if (policyName[0] == '_') {
        throw SnapshotManagementException(message = "Policy name must not start with '_'")
    }
    if (policyName.lowercase(Locale.ROOT) != policyName) {
        throw SnapshotManagementException(message = "Policy name must be lowercase")
    }
    if (!Strings.validFileName(policyName)) {
        throw SnapshotManagementException(
            message = "Policy name must not contain the following characters " + Strings.INVALID_FILENAME_CHARS
        )
    }
}
