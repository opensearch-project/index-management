/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.resthandler

import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import java.time.Instant
import java.time.temporal.ChronoUnit

class RestExplainSnapshotManagementIT : SnapshotManagementRestTestCase() {

    @Suppress("UNCHECKED_CAST")
    fun `test explaining a snapshot management policy`() {
        val smPolicy = createSMPolicy(
            randomSMPolicy().copy(
                jobEnabled = true,
                jobEnabledTime = Instant.now(),
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            )
        )
        updateSMPolicyStartTime(smPolicy)
        val smMetadata = waitFor {
            val explainResponse = explainSMPolicy(smPolicy.policyName)
            val responseMap = createParser(XContentType.JSON.xContent(), explainResponse.entity.content).map() as Map<String, Map<String, Any>>
            assertTrue(responseMap.containsKey(smPolicy.policyName))
            val responseSpecificPolicy = responseMap[smPolicy.policyName] as Map<String, Any>
            assertTrue(responseSpecificPolicy.containsKey(SMMetadata.SM_METADATA_TYPE))
            val smMetadataString = responseSpecificPolicy[SMMetadata.SM_METADATA_TYPE] as Map<String, Any>
            // TODO validate SM Metadata
        }
    }
}
