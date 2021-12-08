/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import org.opensearch.test.OpenSearchTestCase

class AttemptCreateRollupJobStepTests : OpenSearchTestCase() {

    /*private val rollupActionConfig = randomRollupActionConfig()
    private val indexName = "test"
    private val rollupId: String = rollupActionConfig.ismRollup.toRollup(indexName).id
    private val client: Client = mock()
    private val clusterService: ClusterService = mock()
    private val metadata = ManagedIndexMetaData(
        indexName, "indexUuid", "policy_id", null, null, null, null, null, null,
        ActionMetaData(AttemptCreateRollupJobStep.name, 1, 0, false, 0, null, ActionProperties(rollupId = rollupId)), null, null, null
    )
    private val step = AttemptCreateRollupJobStep(clusterService, client, rollupActionConfig.ismRollup, metadata)

    fun `test process failure`() {
        step.processFailure(rollupId, Exception("dummy-error"))
        val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Error message is not expected",
            AttemptCreateRollupJobStep.getFailedMessage(rollupId, indexName),
            updatedManagedIndexMetaData.info?.get("message")
        )
    }

    fun `test isIdempotent`() {
        assertTrue(step.isIdempotent())
    }*/
}
