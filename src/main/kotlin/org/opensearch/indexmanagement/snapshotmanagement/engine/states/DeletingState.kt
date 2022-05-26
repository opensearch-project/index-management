/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.snapshots.SnapshotMissingException
import java.time.Instant
import java.time.Instant.now

object DeletingState : State {

    override val continuous: Boolean = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        val res: AcknowledgedResponse
        val snapshotToDelete: List<SMMetadata.SnapshotInfo>

        val getSnapshots = try {
            client.getSnapshots(
                smDocIdToPolicyName(job.id) + "*",
                job.snapshotConfig["repository"] as String
            )
        } catch (ex: SnapshotMissingException) {
            log.warn("No snapshots found under policy while getting snapshots to decide which snapshots to delete.")
            return SMResult.Failure(ex, WorkflowType.DELETION)
        } catch (ex: Exception) {
            log.error("Caught exception while getting snapshots to decide which snapshots to delete.", ex)
            return SMResult.Retry(WorkflowType.DELETION)
        }

        snapshotToDelete = findSnapshotsToDelete(getSnapshots, job.deletion.condition, log)
        log.info("sm dev: Going to delete: ${snapshotToDelete.map { it.name }}")

        if (snapshotToDelete.isNotEmpty()) {
            try {
                val req = DeleteSnapshotRequest(
                    job.snapshotConfig["repository"] as String,
                    *snapshotToDelete.map { it.name }.toTypedArray()
                )
                res = client.admin().cluster().suspendUntil { deleteSnapshot(req, it) }
                log.info("sm dev: Delete snapshot acknowledged: ${res.isAcknowledged}.")
            } catch (ex: Exception) {
                return SMResult.Failure(ex, WorkflowType.DELETION, notifiable = true)
            }
        }

        val metadataToSave = SMMetadata.Builder(metadata)
            .resetRetry(deletion = true)
        if (snapshotToDelete.isNotEmpty())
            metadataToSave.deletion(
                startedTime = now(),
                snapshotInfo = snapshotToDelete,
            )

        return SMResult.Next(metadataToSave.build())
    }

    /**
     * Based on the condition to delete, find snapshots to be deleted
     *
     * Logic:
     *   outdated snapshots: snapshot.startedTime < now - max_age
     *   keep at least min_count of snapshots even it's outdated
     *   keep at most max_count of snapshots
     */
    private fun findSnapshotsToDelete(snapshots: List<SnapshotInfo>, deleteCondition: SMPolicy.DeleteCondition, log: Logger): List<SMMetadata.SnapshotInfo> {
        val snapshotInfos = snapshots.map {
            SMMetadata.SnapshotInfo(
                it.snapshotId().name,
                Instant.ofEpochMilli(it.startTime()),
                Instant.ofEpochMilli(it.endTime()),
            )
        }.sortedBy { it.startTime } // start_time will always exist along with snapshotId
        log.info("sm dev: snapshotInfos $snapshotInfos")

        var thresholdCount = 0

        if (deleteCondition.maxAge != null) {
            val timeThreshold = now().minusSeconds(deleteCondition.maxAge.seconds())
            log.info("sm dev: time threshold: $timeThreshold")
            val thresholdSnapshot = snapshotInfos.findLast { it.startTime.isBefore(timeThreshold) }
            log.info("sm dev: thresholdSnapshot: $thresholdSnapshot")
            thresholdCount = snapshotInfos.indexOf(thresholdSnapshot) + 1
            log.info("sm dev: thresholdCount: $thresholdCount")
            val minCount = deleteCondition.minCount ?: SMPolicy.DeleteCondition.DEFAULT_MIN_COUNT
            if (snapshotInfos.size - thresholdCount < minCount) {
                thresholdCount = snapshotInfos.size - minCount
            }
        }

        if (snapshotInfos.size - thresholdCount > deleteCondition.maxCount) {
            thresholdCount = snapshotInfos.size - deleteCondition.maxCount
        }

        return snapshotInfos.subList(0, thresholdCount)
    }
}
