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
import org.opensearch.indexmanagement.indexstatemanagement.step.transform.AttemptCreateTransformJobStep
import org.opensearch.indexmanagement.indexstatemanagement.step.transform.WaitForTransformCompletionStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.TransformActionProperties
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.model.TransformStats
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class WaitForTransformCompletionStepTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val transformId: String = "dummy-id"
    private val indexName: String = "test"
    private val metadata =
        ManagedIndexMetaData(
            indexName,
            "indexUuid",
            "policy_id",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            ActionMetaData(
                AttemptCreateTransformJobStep.name, 1, 0, false, 0, null,
                ActionProperties(transformActionProperties = TransformActionProperties(transformId)),
            ),
            null,
            null,
            null,
        )
    private val transformMetadata =
        TransformMetadata(
            id = NO_ID,
            transformId = transformId,
            lastUpdatedAt = Instant.now(),
            status = TransformMetadata.Status.FINISHED,
            stats = TransformStats(1, 1, 1, 1, 1),
        )
    private val client: Client = mock()
    private val step = WaitForTransformCompletionStep()
    private val lockService: LockService = LockService(mock(), clusterService)

    fun `test wait for transform when missing transform id`() {
        val actionMetadata = metadata.actionMetaData!!.copy(actionProperties = ActionProperties())
        val metadata = metadata.copy(actionMetaData = actionMetadata)
        val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
        val step = WaitForTransformCompletionStep()

        runBlocking {
            step.preExecute(logger, context).execute()
        }

        val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing failure message",
            WaitForTransformCompletionStep.getMissingTransformJobMessage(indexName),
            updatedManagedIndexMetaData.info?.get("message"),
        )
    }

    fun `test process transform metadata FAILED status`() {
        val transformMetadata = transformMetadata.copy(status = TransformMetadata.Status.FAILED)
        step.processTransformMetadataStatus(transformId, indexName, transformMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updateManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing failure message",
            WaitForTransformCompletionStep.getJobFailedMessage(transformId, indexName),
            updateManagedIndexMetaData.info?.get("message"),
        )
    }

    fun `test process transform metadata STOPPED status`() {
        val transformMetadata = transformMetadata.copy(status = TransformMetadata.Status.STOPPED)
        step.processTransformMetadataStatus(transformId, indexName, transformMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updateManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing failure message",
            WaitForTransformCompletionStep.getJobFailedMessage(transformId, indexName),
            updateManagedIndexMetaData.info?.get("message"),
        )
        assertEquals("Mismatch in cause", WaitForTransformCompletionStep.JOB_STOPPED_MESSAGE, updateManagedIndexMetaData.info?.get("cause"))
    }

    fun `test process transform metadata INIT status`() {
        val transformMetadata = transformMetadata.copy(status = TransformMetadata.Status.INIT)
        step.processTransformMetadataStatus(transformId, indexName, transformMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals(
            "Step status is not CONDITION_NOT_MET",
            Step.StepStatus.CONDITION_NOT_MET,
            updateManagedIndexMetaData.stepMetaData?.stepStatus,
        )
        assertEquals(
            "Missing processing message",
            WaitForTransformCompletionStep.getJobProcessingMessage(transformId, indexName),
            updateManagedIndexMetaData.info?.get("message"),
        )
    }

    fun `test process transform metadata STARTED status`() {
        val transformMetadata = transformMetadata.copy(status = TransformMetadata.Status.STARTED)
        step.processTransformMetadataStatus(transformId, indexName, transformMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updateManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing processing message",
            WaitForTransformCompletionStep.getJobProcessingMessage(transformId, indexName),
            updateManagedIndexMetaData.info?.get("message"),
        )
    }

    fun `test process transform metadata FINISHED status`() {
        val transformMetadata = transformMetadata.copy(status = TransformMetadata.Status.FINISHED)
        step.processTransformMetadataStatus(transformId, indexName, transformMetadata)

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updateManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Missing processing message",
            WaitForTransformCompletionStep.getJobCompletionMessage(transformId, indexName),
            updateManagedIndexMetaData.info?.get("message"),
        )
    }

    fun `test process failure`() {
        step.processFailure(transformId, indexName, Exception("dummy-exception"))

        val updateManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Mismatch in cause", "dummy-exception", updateManagedIndexMetaData.info?.get("cause"))
        assertEquals(
            "Mismatch in message",
            WaitForTransformCompletionStep.getFailedMessage(transformId, indexName),
            updateManagedIndexMetaData.info?.get("message"),
        )
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updateManagedIndexMetaData.stepMetaData?.stepStatus)
    }

    fun `test isIdempotent`() {
        assertTrue(step.isIdempotent())
    }
}
