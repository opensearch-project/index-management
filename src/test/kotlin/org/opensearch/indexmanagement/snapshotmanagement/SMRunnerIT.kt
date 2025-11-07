/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import java.time.Instant
import java.time.Instant.now
import java.time.temporal.ChronoUnit

class SMRunnerIT : SnapshotManagementRestTestCase() {
    fun `test overall workflow`() {
        createRepository("repo")

        val smPolicy =
            randomSMPolicy(
                creationSchedule = CronSchedule("* * * * *", randomZone()),
                jobSchedule = IntervalSchedule(now(), 1, ChronoUnit.MINUTES),
                jobEnabled = true,
                jobEnabledTime = now(),
            )
        val policyName = smPolicy.policyName
        val response = client().makeRequest("POST", "$SM_POLICIES_URI/$policyName", emptyMap(), smPolicy.toHttpEntity())
        assertEquals("Create SM policy failed", RestStatus.CREATED, response.restStatus())

        // Initialization
        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNotNull(explainMetadata.creation!!.trigger.time)
        }

        // Create condition met
        updateSMPolicyStartTime(smPolicy)
        updateSMMetadata(getSMPolicy(smPolicy.policyName))
        waitFor(timeout = Instant.ofEpochSecond(180)) {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNotNull(explainMetadata.creation!!.started)
            assertEquals(SMMetadata.LatestExecution.Status.IN_PROGRESS, explainMetadata.creation.latestExecution!!.status)
        }

        // Snapshot has been created successfully
        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNull(explainMetadata.creation!!.started)
            assertEquals(SMMetadata.LatestExecution.Status.SUCCESS, explainMetadata.creation.latestExecution!!.status)
        }
    }

    fun `test deletion workflow with snapshot pattern`() {
        val repoName = "test-repo"
        val indexName = "test-index"
        val policyName = "test-policy-deletion"

        createRepository(repoName)
        createIndex(indexName, Settings.EMPTY)

        // Clear any existing snapshots in the repository
        deleteAllSnapshotsInRepository(repoName)

        // Create 4 external snapshots that match the pattern
        val externalSnapshots = mutableListOf<String>()
        for (i in 1..4) {
            val snapshotName = "external-backup-snapshot-$i"
            createSnapshot(repoName, snapshotName, indexName)
            externalSnapshots.add(snapshotName)
            Thread.sleep(100) // Ensure timestamp difference between snapshots
        }

        // Verify all external snapshots exist
        val snapshotsBefore = getSnapshotsInRepository(repoName)
        assertEquals("Should have 4 snapshots", 4, snapshotsBefore.size)
        externalSnapshots.forEach { snapshot ->
            assertTrue("Should contain $snapshot", snapshotsBefore.contains(snapshot))
        }

        // Create SM policy with deletion workflow that includes snapshot_pattern
        val smPolicy =
            randomSMPolicy(
                policyName = policyName,
                creationSchedule = CronSchedule("0 0 * * *", randomZone()),
                deletionSchedule = CronSchedule("* * * * *", randomZone()),
                deletionMaxAge = org.opensearch.common.unit.TimeValue.timeValueMillis(1), // Delete old snapshots
                deletionMinCount = 1, // Keep at least 1 snapshot
                snapshotPattern = "external-backup-*", // This is the key feature being tested
                snapshotConfig = mutableMapOf("repository" to repoName),
                jobSchedule = IntervalSchedule(now(), 1, ChronoUnit.MINUTES),
                jobEnabled = true,
                jobEnabledTime = now(),
            )

        val response = client().makeRequest("POST", "$SM_POLICIES_URI/$policyName", emptyMap(), smPolicy.toHttpEntity())
        assertEquals("Create SM policy failed", RestStatus.CREATED, response.restStatus())

        // Initialization - wait for deletion trigger to be set
        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNotNull("Deletion trigger should be initialized", explainMetadata.deletion?.trigger?.time)
        }

        // Create condition met
        updateSMPolicyStartTime(smPolicy)
        updateSMMetadata(getSMPolicy(smPolicy.policyName))
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNotNull(explainMetadata.creation!!.started)
            assertEquals(SMMetadata.LatestExecution.Status.IN_PROGRESS, explainMetadata.creation.latestExecution!!.status)
        }

        // Snapshot has been created successfully
        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNull(explainMetadata.creation!!.started)
            assertEquals(SMMetadata.LatestExecution.Status.SUCCESS, explainMetadata.creation.latestExecution!!.status)
        }

        // Trigger deletion workflow - wait for it to start
        updateSMPolicyStartTime(smPolicy)
        waitFor(timeout = Instant.ofEpochSecond(120)) {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNotNull("Deletion should have started", explainMetadata.deletion?.started)
        }

        //  Wait for deletion to complete successfully
        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNull("Deletion should have finished (started should be null)", explainMetadata.deletion?.started)
            assertEquals(
                "Deletion should complete successfully",
                SMMetadata.LatestExecution.Status.SUCCESS,
                explainMetadata.deletion?.latestExecution?.status,
            )
        }

        logger.info("Deletion workflow with snapshot_pattern completed successfully")

        // Verify the deletion behavior - 3 oldest snapshots should be deleted, keeping minCount=1
        val snapshotsAfter = getSnapshotsInRepository(repoName)
        assertEquals("Should have 1 snapshot remaining (minCount=1)", 1, snapshotsAfter.size)
    }
}
