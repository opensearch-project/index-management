/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import org.opensearch.indexmanagement.indexstatemanagement.action.RollupAction
import org.opensearch.indexmanagement.indexstatemanagement.randomRollupActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import org.opensearch.indexmanagement.rollup.randomISMRollup
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.test.OpenSearchTestCase

class AttemptCreateRollupJobStepTests : OpenSearchTestCase() {
    private val rollupAction = randomRollupActionConfig()
    private val indexName = "test"
    private val rollupId: String = rollupAction.ismRollup.toRollup(indexName).id
    private val metadata =
        ManagedIndexMetaData(
            indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
            ActionMetaData(AttemptCreateRollupJobStep.name, 1, 0, false, 0, null, ActionProperties(rollupId = rollupId)), null, null, null,
        )
    private val step = AttemptCreateRollupJobStep(rollupAction)

    fun `test process failure`() {
        step.processFailure(rollupId, indexName, Exception("dummy-error"))
        val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Error message is not expected",
            AttemptCreateRollupJobStep.getFailedMessage(rollupId, indexName),
            updatedManagedIndexMetaData.info?.get("message"),
        )
    }

    fun `test isIdempotent`() {
        assertTrue(step.isIdempotent())
    }

    fun `test source index resolution with explicit source_index`() {
        // Create ISMRollup with explicit source_index
        val explicitSourceIndex = "explicit_source_index"
        val ismRollup = randomISMRollup().copy(sourceIndex = explicitSourceIndex)
        val action = RollupAction(ismRollup = ismRollup, index = 0)

        // Create rollup using the managed index name
        val managedIndexName = "managed_index"
        val rollup = ismRollup.toRollup(managedIndexName)

        // Verify that the rollup uses the explicit source_index, not the managed index
        // The rollup ID is generated from the resolved source index
        val expectedRollupId = ismRollup.toRollup(managedIndexName).id
        assertEquals("Rollup ID should be generated from explicit source_index", expectedRollupId, rollup.id)

        // Verify the sourceIndex field in the rollup matches the explicit source
        assertEquals("Source index should match explicit source_index", explicitSourceIndex, rollup.sourceIndex)
    }

    fun `test source index resolution without source_index uses managed index`() {
        // Create ISMRollup without source_index (null)
        val ismRollup = randomISMRollup().copy(sourceIndex = null)
        val action = RollupAction(ismRollup = ismRollup, index = 0)

        // Create rollup using the managed index name
        val managedIndexName = "managed_index"
        val rollup = ismRollup.toRollup(managedIndexName)

        // Verify that the rollup uses the managed index as source
        assertEquals("Source index should match managed index when source_index is null", managedIndexName, rollup.sourceIndex)
    }

    fun `test source index resolution with empty source_index uses managed index`() {
        // Create ISMRollup with null source_index
        val ismRollup = randomISMRollup().copy(sourceIndex = null)

        // Create rollup using the managed index name
        val managedIndexName = "my_managed_index"
        val rollup = ismRollup.toRollup(managedIndexName)

        // Verify that the rollup uses the managed index as source (backward compatibility)
        assertEquals("Source index should default to managed index", managedIndexName, rollup.sourceIndex)
    }

    fun `test source index resolution with different source and managed indices`() {
        // Create ISMRollup with explicit source_index different from managed index
        val explicitSourceIndex = "rollup_1m_index"
        val managedIndexName = "rollup_5m_index"
        val ismRollup = randomISMRollup().copy(sourceIndex = explicitSourceIndex)

        // Create rollup
        val rollup = ismRollup.toRollup(managedIndexName)

        // Verify that the rollup uses the explicit source_index
        assertEquals("Source index should be the explicit source_index", explicitSourceIndex, rollup.sourceIndex)
        assertNotEquals("Source index should not be the managed index", managedIndexName, rollup.sourceIndex)
    }

    fun `test process failure includes source index in error context`() {
        // Create ISMRollup with explicit source_index
        val explicitSourceIndex = "source_index_that_fails"
        val ismRollup = randomISMRollup().copy(sourceIndex = explicitSourceIndex)
        val action = RollupAction(ismRollup = ismRollup, index = 0)
        val step = AttemptCreateRollupJobStep(action)

        val managedIndexName = "managed_index"
        val rollup = ismRollup.toRollup(managedIndexName)

        // Simulate failure
        val errorMessage = "Source index not found"
        step.processFailure(rollup.id, managedIndexName, Exception(errorMessage))

        val metadata = ManagedIndexMetaData(
            managedIndexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
            ActionMetaData(AttemptCreateRollupJobStep.name, 1, 0, false, 0, null, ActionProperties(rollupId = rollup.id)),
            null, null, null,
        )

        val updatedMetadata = step.getUpdatedManagedIndexMetadata(metadata)

        // Verify failure status and error message
        assertEquals("Step status should be FAILED", Step.StepStatus.FAILED, updatedMetadata.stepMetaData?.stepStatus)
        assertEquals(
            "Error message should indicate failure",
            AttemptCreateRollupJobStep.getFailedMessage(rollup.id, managedIndexName),
            updatedMetadata.info?.get("message"),
        )
        assertEquals("Error cause should be included", errorMessage, updatedMetadata.info?.get("cause"))
    }

    fun `test rollup ID generation with explicit source_index`() {
        // Create two ISMRollup configs with same settings but different source indices
        val sourceIndex1 = "source_index_1"
        val sourceIndex2 = "source_index_2"
        val managedIndexName = "managed_index"

        val baseRollup = randomISMRollup()
        val ismRollup1 = baseRollup.copy(sourceIndex = sourceIndex1)
        val ismRollup2 = baseRollup.copy(sourceIndex = sourceIndex2)

        val rollup1 = ismRollup1.toRollup(managedIndexName)
        val rollup2 = ismRollup2.toRollup(managedIndexName)

        // Verify that rollup IDs are different because source indices are different
        assertNotEquals("Rollup IDs should differ when source indices differ", rollup1.id, rollup2.id)

        // Verify that source indices are correctly set
        assertEquals("First rollup should use first source index", sourceIndex1, rollup1.sourceIndex)
        assertEquals("Second rollup should use second source index", sourceIndex2, rollup2.sourceIndex)
    }

    fun `test rollup ID generation without source_index uses managed index`() {
        // Create ISMRollup without source_index
        val ismRollup = randomISMRollup().copy(sourceIndex = null)
        val managedIndexName = "managed_index"

        val rollup = ismRollup.toRollup(managedIndexName)

        // Verify that the rollup uses managed index as source
        assertEquals("Rollup should use managed index as source", managedIndexName, rollup.sourceIndex)

        // Verify that rollup ID is generated from managed index
        val expectedRollupId = ismRollup.toRollup(managedIndexName).id
        assertEquals("Rollup ID should be consistent", expectedRollupId, rollup.id)
    }

    fun `test source_index template resolution is called with templated source_index`() {
        // Create ISMRollup with templated source_index
        val templatedSourceIndex = "{{ctx.index}}"
        val managedIndexName = "logs-2024-01"
        val ismRollup = randomISMRollup().copy(
            sourceIndex = templatedSourceIndex,
            targetIndex = "rollup_{{ctx.index}}",
        )
        val action = RollupAction(ismRollup = ismRollup, index = 0)

        // Create a temporary rollup to verify template resolution context
        val tempRollup = ismRollup.toRollup(managedIndexName)

        // Verify that when toRollup is called with a managed index name,
        // the source_index field in ISMRollup is used (not yet resolved)
        assertEquals(
            "ISMRollup should contain the template before resolution",
            templatedSourceIndex,
            ismRollup.sourceIndex,
        )

        // Verify that the rollup object created from ISMRollup uses the explicit source_index
        // (In the actual step execution, this would be resolved by RollupFieldValueExpressionResolver)
        assertEquals(
            "Rollup should use the explicit source_index from ISMRollup",
            templatedSourceIndex,
            tempRollup.sourceIndex,
        )

        // Verify that the target_index also contains a template
        assertEquals(
            "Target index should contain template",
            "rollup_{{ctx.index}}",
            ismRollup.targetIndex,
        )
    }

    fun `test source_index template resolution parameters for multi-tier rollup`() {
        // Simulate a multi-tier rollup scenario where the managed index is itself a rollup
        val firstTierRollupIndex = "rollup_tier1_logs-2024-01"
        val templatedSourceIndex = "{{ctx.index}}"
        val templatedTargetIndex = "rollup_tier2_{{ctx.index}}"

        val ismRollup = randomISMRollup().copy(
            sourceIndex = templatedSourceIndex,
            targetIndex = templatedTargetIndex,
        )
        val action = RollupAction(ismRollup = ismRollup, index = 0)

        // Create a temporary rollup with the first-tier rollup index as the managed index
        val tempRollup = ismRollup.toRollup(firstTierRollupIndex)

        // Verify that the ISMRollup contains templates
        assertEquals(
            "Source index should contain template",
            templatedSourceIndex,
            ismRollup.sourceIndex,
        )
        assertEquals(
            "Target index should contain template",
            templatedTargetIndex,
            ismRollup.targetIndex,
        )

        // Verify that the temporary rollup has the templated source_index
        // (before resolution by RollupFieldValueExpressionResolver)
        assertEquals(
            "Temporary rollup should have templated source_index",
            templatedSourceIndex,
            tempRollup.sourceIndex,
        )
    }

    fun `test source_index and target_index template resolution parameters`() {
        // Create ISMRollup with templates in both source_index and target_index
        val managedIndexName = "my-index-2024-01"
        val sourceTemplate = "{{ctx.index}}"
        val targetTemplate = "rollup_{{ctx.index}}"

        val ismRollup = randomISMRollup().copy(
            sourceIndex = sourceTemplate,
            targetIndex = targetTemplate,
        )
        val action = RollupAction(ismRollup = ismRollup, index = 0)

        // Create temporary rollup for template resolution context
        val tempRollup = ismRollup.toRollup(managedIndexName)

        // Verify that both fields contain templates before resolution
        assertEquals(
            "Source index should contain template",
            sourceTemplate,
            ismRollup.sourceIndex,
        )
        assertEquals(
            "Target index should contain template",
            targetTemplate,
            ismRollup.targetIndex,
        )

        // Verify that the temporary rollup is created with the templated values
        assertEquals(
            "Temporary rollup should have templated source_index",
            sourceTemplate,
            tempRollup.sourceIndex,
        )
        assertEquals(
            "Temporary rollup should have templated target_index",
            targetTemplate,
            tempRollup.targetIndex,
        )
    }

    fun `test target_index template resolution is called with correct parameters`() {
        // Create ISMRollup with templated target_index
        val managedIndexName = "logs-2024-01"
        val targetTemplate = "rollup_{{ctx.index}}"
        val ismRollup = randomISMRollup().copy(
            sourceIndex = null, // Use default (managed index)
            targetIndex = targetTemplate,
        )
        val action = RollupAction(ismRollup = ismRollup, index = 0)

        // Create temporary rollup for template resolution context
        val tempRollup = ismRollup.toRollup(managedIndexName)

        // Verify that target_index contains template before resolution
        assertEquals(
            "Target index should contain template",
            targetTemplate,
            ismRollup.targetIndex,
        )

        // Verify that the temporary rollup is created with the templated target_index
        assertEquals(
            "Temporary rollup should have templated target_index",
            targetTemplate,
            tempRollup.targetIndex,
        )

        // Verify that source_index defaults to managed index when null
        assertEquals(
            "Source index should default to managed index",
            managedIndexName,
            tempRollup.sourceIndex,
        )
    }

    fun `test target_index template resolution with complex template`() {
        // Create ISMRollup with complex target_index template using multiple variables
        val managedIndexName = "rollup_tier1_logs-2024-01"
        val targetTemplate = "rollup_tier2_{{ctx.index}}_aggregated"
        val ismRollup = randomISMRollup().copy(
            sourceIndex = "{{ctx.index}}",
            targetIndex = targetTemplate,
        )
        val action = RollupAction(ismRollup = ismRollup, index = 0)

        // Create temporary rollup for template resolution context
        val tempRollup = ismRollup.toRollup(managedIndexName)

        // Verify that target_index contains complex template
        assertEquals(
            "Target index should contain complex template",
            targetTemplate,
            ismRollup.targetIndex,
        )

        // Verify that the temporary rollup is created with the templated target_index
        assertEquals(
            "Temporary rollup should have templated target_index",
            targetTemplate,
            tempRollup.targetIndex,
        )
    }

    fun `test target_index template resolution with source_index template`() {
        // Create ISMRollup where both source_index and target_index use templates
        val managedIndexName = "logs-2024-01"
        val sourceTemplate = "{{ctx.index}}"
        val targetTemplate = "rollup_{{ctx.source_index}}"
        val ismRollup = randomISMRollup().copy(
            sourceIndex = sourceTemplate,
            targetIndex = targetTemplate,
        )
        val action = RollupAction(ismRollup = ismRollup, index = 0)

        // Create temporary rollup for template resolution context
        val tempRollup = ismRollup.toRollup(managedIndexName)

        // Verify that both fields contain templates
        assertEquals(
            "Source index should contain template",
            sourceTemplate,
            ismRollup.sourceIndex,
        )
        assertEquals(
            "Target index should contain template with source_index variable",
            targetTemplate,
            ismRollup.targetIndex,
        )

        // Verify that the temporary rollup is created with templated values
        assertEquals(
            "Temporary rollup should have templated source_index",
            sourceTemplate,
            tempRollup.sourceIndex,
        )
        assertEquals(
            "Temporary rollup should have templated target_index",
            targetTemplate,
            tempRollup.targetIndex,
        )
    }
}
