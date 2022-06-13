/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit

class SMRunnerIT : SnapshotManagementRestTestCase() {

    fun `test overall workflow`() {
        createRepository("repo")

        val smPolicy = randomSMPolicy(
            creationSchedule = CronSchedule("* * * * *", randomZone()),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabled = true,
        )
        val policyName = smPolicy.policyName
        val response = client().makeRequest("POST", "$SM_POLICIES_URI/$policyName", emptyMap(), smPolicy.toHttpEntity())
        assertEquals("Create SM policy failed", RestStatus.CREATED, response.restStatus())

        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content)
            assertNotNull(explainMetadata.creation!!.trigger.time)
        }

        // Wait for cron schedule to meet
        Thread.sleep(41_000L)

        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content)
            assertNotNull(explainMetadata.creation!!.started)
            assertEquals(SMMetadata.LatestExecution.Status.IN_PROGRESS, explainMetadata.creation.latestExecution!!.status)
        }

        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content)
            assertNull(explainMetadata.creation!!.started)
            assertEquals(SMMetadata.LatestExecution.Status.SUCCESS, explainMetadata.creation.latestExecution!!.status)
        }
    }
}
