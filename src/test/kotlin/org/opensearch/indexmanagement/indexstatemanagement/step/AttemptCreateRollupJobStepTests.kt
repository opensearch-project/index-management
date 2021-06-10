/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.mock
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import org.opensearch.indexmanagement.indexstatemanagement.randomRollupActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import org.opensearch.test.OpenSearchTestCase
import java.lang.Exception

class AttemptCreateRollupJobStepTests : OpenSearchTestCase() {

    private val rollupActionConfig = randomRollupActionConfig()
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
    }
}
