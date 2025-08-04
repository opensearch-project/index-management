/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.snapshots.SnapshotInfo

object DeletionStateUtils {

    /**
     * Get pattern-based snapshots if pattern is specified
     *
     * @param context The state machine context
     * @param metadataBuilder The current metadata builder
     * @return A pair containing the pattern snapshots list and updated metadata builder, or null if failed
     */
    suspend fun getPatternSnapshots(
        context: SMStateMachine,
        metadataBuilder: SMMetadata.Builder,
    ): Pair<List<SnapshotInfo>, SMMetadata.Builder>? {
        val client = context.client
        val job = context.job
        val log = context.log
        val repository = job.snapshotConfig["repository"] as String

        return if (job.deletion?.snapshotPattern != null) {
            try {
                val patternSnapshots = client.getSnapshots(job.deletion.snapshotPattern, repository)
                Pair(patternSnapshots, metadataBuilder)
            } catch (ex: Exception) {
                log.error("Caught exception while getting snapshots for pattern ${job.deletion.snapshotPattern}.", ex)
                val updatedMetadataBuilder = metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.RETRYING,
                    message = "Caught exception while getting snapshots for pattern ${job.deletion.snapshotPattern}.",
                    cause = ex,
                )
                null
            }
        } else {
            Pair(emptyList(), metadataBuilder)
        }
    }
}
