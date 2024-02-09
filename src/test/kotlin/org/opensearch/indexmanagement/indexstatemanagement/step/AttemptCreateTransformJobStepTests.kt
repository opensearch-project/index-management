/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import org.opensearch.indexmanagement.indexstatemanagement.randomTransformActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.transform.AttemptCreateTransformJobStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.TransformActionProperties
import org.opensearch.test.OpenSearchTestCase

class AttemptCreateTransformJobStepTests : OpenSearchTestCase() {

    private val transformAction = randomTransformActionConfig()
    private val indexName = "test"
    private val transformId: String = transformAction.ismTransform.toTransform(indexName).id
    private val metadata = ManagedIndexMetaData(
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
    private val step = AttemptCreateTransformJobStep(transformAction)

    fun `test process failure`() {
        step.processFailure(transformId, indexName, Exception("dummy-error"))
        val updatedManagedIndexMedaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals(
            "Step status is not FAILED",
            Step.StepStatus.FAILED,
            updatedManagedIndexMedaData.stepMetaData?.stepStatus,
        )
        assertEquals(
            "Error message is not expected",
            AttemptCreateTransformJobStep.getFailedMessage(transformId, indexName),
            updatedManagedIndexMedaData.info?.get("message"),
        )
        assertNull(
            "TransformId in action properties is not cleaned up",
            updatedManagedIndexMedaData.actionMetaData?.actionProperties?.transformActionProperties?.transformId,
        )
    }

    fun `test isIdempotent`() {
        assertTrue(step.isIdempotent())
    }
}
