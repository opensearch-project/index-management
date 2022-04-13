/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.mock
import kotlinx.coroutines.runBlocking
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.RollupStats
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class WaitForRollupCompletionStepTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val rollupId: String = "dummy-id"
    private val indexName: String = "test"
    private val metadata = ManagedIndexMetaData(
        indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
        ActionMetaData
        (WaitForRollupCompletionStep.name, 1, 0, false, 0, null, ActionProperties(rollupId = rollupId)),
        null, null, null
    )
    private val rollupMetadata = RollupMetadata(
        rollupID = rollupId, lastUpdatedTime = Instant.now(), status = RollupMetadata.Status.FINISHED,
        stats = RollupStats(1, 1, 1, 1, 1)
    )
    private val client: Client = mock()
    private val step = WaitForRollupCompletionStep()
    private val lockService: LockService = LockService(mock(), clusterService)

    fun `test wait for rollup when missing rollup id`() {
        val actionMetadata = metadata.actionMetaData!!.copy(actionProperties = ActionProperties())
        val metadata = metadata.copy(actionMetaData = actionMetadata)
        val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
        val step = WaitForRollupCompletionStep()

        runBlocking {
            step.preExecute(logger, context).execute()
        }

        val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing failure message",
            WaitForRollupCompletionStep.getMissingRollupJobMessage(indexName),
            updatedManagedIndexMetaData.info?.get("message")
        )
    }

    fun `test process rollup metadata FAILED status`() {
        val rollupMetadata = rollupMetadata.copy(status = RollupMetadata.Status.FAILED)
        step.processRollupMetadataStatus(rollupId, indexName, rollupMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updateManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing failure message",
            WaitForRollupCompletionStep.getJobFailedMessage(rollupId, indexName),
            updateManagedIndexMetaData.info?.get("message")
        )
        assertEquals("Missing rollup failed action property", true, updateManagedIndexMetaData.actionMetaData?.actionProperties?.hasRollupFailed)
    }

    fun `test process rollup metadata STOPPED status`() {
        val rollupMetadata = rollupMetadata.copy(status = RollupMetadata.Status.STOPPED)
        step.processRollupMetadataStatus(rollupId, indexName, rollupMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updateManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing failure message",
            WaitForRollupCompletionStep.getJobFailedMessage(rollupId, indexName),
            updateManagedIndexMetaData.info?.get("message")
        )
        assertEquals("Missing rollup failed action property", true, updateManagedIndexMetaData.actionMetaData?.actionProperties?.hasRollupFailed)
        assertEquals("Mismatch in cause", WaitForRollupCompletionStep.JOB_STOPPED_MESSAGE, updateManagedIndexMetaData.info?.get("cause"))
    }

    fun `test process rollup metadata INIT status`() {
        val rollupMetadata = rollupMetadata.copy(status = RollupMetadata.Status.INIT)
        step.processRollupMetadataStatus(rollupId, indexName, rollupMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updateManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing processing message",
            WaitForRollupCompletionStep.getJobProcessingMessage(rollupId, indexName),
            updateManagedIndexMetaData.info?.get("message")
        )
        assertNull("rollup failed property is not null", updateManagedIndexMetaData.actionMetaData?.actionProperties?.hasRollupFailed)
    }

    fun `test process rollup metadata STARTED status`() {
        val rollupMetadata = rollupMetadata.copy(status = RollupMetadata.Status.STARTED)
        step.processRollupMetadataStatus(rollupId, indexName, rollupMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updateManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing processing message",
            WaitForRollupCompletionStep.getJobProcessingMessage(rollupId, indexName),
            updateManagedIndexMetaData.info?.get("message")
        )
        assertNull("rollup failed property is not null", updateManagedIndexMetaData.actionMetaData?.actionProperties?.hasRollupFailed)
    }

    fun `test process rollup metadata FINISHED status`() {
        val rollupMetadata = rollupMetadata.copy(status = RollupMetadata.Status.FINISHED)
        step.processRollupMetadataStatus(rollupId, indexName, rollupMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updateManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing processing message",
            WaitForRollupCompletionStep.getJobCompletionMessage(rollupId, indexName),
            updateManagedIndexMetaData.info?.get("message")
        )
        assertNull("rollup failed property is not null", updateManagedIndexMetaData.actionMetaData?.actionProperties?.hasRollupFailed)
    }

    fun `test process rollup metadata RETRY status`() {
        val rollupMetadata = rollupMetadata.copy(status = RollupMetadata.Status.RETRY)
        step.processRollupMetadataStatus(rollupId, indexName, rollupMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updateManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing processing message",
            WaitForRollupCompletionStep.getJobProcessingMessage(rollupId, indexName),
            updateManagedIndexMetaData.info?.get("message")
        )
        assertNull("rollup failed property is not null", updateManagedIndexMetaData.actionMetaData?.actionProperties?.hasRollupFailed)
    }

    fun `test process failure`() {
        step.processFailure(rollupId, indexName, Exception("dummy-exception"))

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Mismatch in cause", "dummy-exception", updateManagedIndexMetaData.info?.get("cause"))
        assertEquals(
            "Mismatch in message",
            WaitForRollupCompletionStep.getFailedMessage(rollupId, indexName),
            updateManagedIndexMetaData.info?.get("message")
        )
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updateManagedIndexMetaData.stepMetaData?.stepStatus)
    }

    fun `test isIdempotent`() {
        assertTrue(step.isIdempotent())
    }
}
