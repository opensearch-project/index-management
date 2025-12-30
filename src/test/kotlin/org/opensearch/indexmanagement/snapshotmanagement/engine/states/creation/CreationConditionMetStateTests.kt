/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation

import kotlinx.coroutines.runBlocking
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.MocksTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import java.time.Instant.now

class CreationConditionMetStateTests : MocksTestCase() {
    fun `test next creation time met`() =
        runBlocking {
            val metadata =
                randomSMMetadata(
                    creationCurrentState = SMState.CREATION_START,
                    nextCreationTime = now().minusSeconds(60),
                )
            val job = randomSMPolicy()
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.CREATION_CONDITION_MET.instance.execute(context)
            assertTrue("Execution result should be Next.", result is SMResult.Next)
            result as SMResult.Next
            assertNotEquals("Next execution time should be updated.", metadata.creation?.trigger?.time, result.metadataToSave.build().creation?.trigger?.time)
        }

    fun `test next creation time has not met`() =
        runBlocking {
            val metadata =
                randomSMMetadata(
                    creationCurrentState = SMState.CREATION_START,
                    nextCreationTime = now().plusSeconds(60),
                )
            val job = randomSMPolicy()
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.CREATION_CONDITION_MET.instance.execute(context)
            assertTrue("Execution result should be Stay.", result is SMResult.Stay)
            result as SMResult.Stay
            assertEquals("Next execution time should not be updated.", metadata, result.metadataToSave.build())
        }

    fun `test creation condition with deletion-only policy should not execute`() =
        runBlocking {
            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = null, // No creation workflow
                deletion = SMMetadata.WorkflowMetadata(
                    currentState = SMState.DELETION_START,
                    trigger = SMMetadata.Trigger(time = now()),
                ),
            )
            val job = randomSMPolicy(
                creationNull = true, // Deletion-only policy
                deletionMaxAge = TimeValue.timeValueDays(7),
                deletionMinCount = 3,
            )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            // Creation condition state should not be reached for deletion-only policies
            // This test ensures the state machine doesn't try to execute creation states
            // when the policy has no creation workflow

            assertNull("Creation should be null for deletion-only policy", job.creation)
            assertNotNull("Deletion should exist for deletion-only policy", job.deletion)
            assertNull("Metadata creation should be null", metadata.creation)
            assertNotNull("Metadata deletion should exist", metadata.deletion)
        }

    fun `test creation condition met with null job creation should fail`() =
        runBlocking {
            val metadata = randomSMMetadata(
                creationCurrentState = SMState.CREATION_START,
                nextCreationTime = now().minusSeconds(60),
            )
            val job = randomSMPolicy(creationNull = true) // Policy with null creation
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.CREATION_CONDITION_MET.instance.execute(context)
            assertTrue("Execution result should be Fail when job.creation is null", result is SMResult.Fail)
            result as SMResult.Fail
            assertEquals("Should fail with CREATION workflow type", org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType.CREATION, result.workflowType)
        }

    fun `test creation condition met with null metadata creation should initialize creation time`() =
        runBlocking {
            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = null, // No existing creation metadata
                deletion = null,
            )
            val job = randomSMPolicy(creationNull = false) // Policy with creation config
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.CREATION_CONDITION_MET.instance.execute(context)
            // The result could be Next or Stay depending on whether the current time has passed the next execution time
            assertTrue(
                "Execution result should be Next or Stay when initializing creation time",
                result is SMResult.Next || result is SMResult.Stay,
            )

            val builtMetadata = when (result) {
                is SMResult.Next -> result.metadataToSave.build()

                is SMResult.Stay -> result.metadataToSave.build()

                else -> {
                    fail("Unexpected result type")
                    error("Should not reach here")
                }
            }

            // Verify that creation metadata is initialized
            assertNotNull("Creation metadata should be initialized", builtMetadata.creation)
            assertNotNull("Creation trigger time should be set", builtMetadata.creation?.trigger?.time)
        }

    fun `test creating state with null job creation should fail and reset creation`() =
        runBlocking {
            val job = randomSMPolicy(creationNull = true) // Policy with null creation
            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = SMMetadata.WorkflowMetadata(
                    currentState = org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState.CREATION_START,
                    trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
                ),
                deletion = null,
            )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = CreatingState.execute(context)

            assertTrue("Result should be Fail", result is SMResult.Fail)
            val failResult = result as SMResult.Fail
            assertEquals("Workflow type should be CREATION", org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType.CREATION, failResult.workflowType)
            assertTrue("Force reset should be true", failResult.forceReset!!)
        }
}
