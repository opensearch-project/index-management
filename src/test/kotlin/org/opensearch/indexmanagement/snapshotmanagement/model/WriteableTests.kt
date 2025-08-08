/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.opensearch.Version
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.randomNotificationConfig
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.test.OpenSearchTestCase

class WriteableTests : OpenSearchTestCase() {
    fun `test sm policy as stream`() {
        val smPolicy = randomSMPolicy(notificationConfig = randomNotificationConfig())
        val out = BytesStreamOutput().also { smPolicy.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedSMPolicy = SMPolicy(sin)
        assertEquals("Round tripping sm policy stream doesn't work", smPolicy, streamedSMPolicy)
    }

    fun `test sm metadata as stream`() {
        val smMetadata = randomSMMetadata()
        val out = BytesStreamOutput().also { smMetadata.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedSMMetadata = SMMetadata(sin)
        assertEquals("Round tripping sm metadata stream doesn't work", smMetadata, streamedSMMetadata)
    }

    fun `test sm metadata as stream with older version requires creation`() {
        // Ensure creation is non-null for older versions
        val smMetadata = randomSMMetadata()
        val out = BytesStreamOutput().also {
            it.version = Version.V_3_1_0
            smMetadata.writeTo(it)
        }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes).also { it.version = Version.V_3_1_0 }
        val streamedSMMetadata = SMMetadata(sin)
        assertEquals("Round tripping sm metadata (older version) doesn't work", smMetadata, streamedSMMetadata)
    }

    fun `test sm metadata as stream with newer version allows nullable creation`() {
        // Build metadata with nullable creation and non-null deletion
        val smMetadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = null,
            deletion = SMMetadata.WorkflowMetadata(
                currentState = SMState.DELETION_START,
                trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
            ),
        )
        val out = BytesStreamOutput().also {
            it.version = Version.V_3_2_0
            smMetadata.writeTo(it)
        }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes).also { it.version = Version.V_3_2_0 }
        val streamedSMMetadata = SMMetadata(sin)
        assertEquals("Round tripping sm metadata (newer version) doesn't work", smMetadata, streamedSMMetadata)
    }

    fun `test sm metadata builder getStartedSnapshots with null creation`() {
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = null,
            deletion = null,
        )
        val builder = SMMetadata.Builder(metadata).workflow(WorkflowType.CREATION)
        assertNull("getStartedSnapshots should return null when creation is null", builder.getStartedSnapshots())
    }

    fun `test sm metadata builder getStartedSnapshots with null deletion`() {
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = null,
            deletion = null,
        )
        val builder = SMMetadata.Builder(metadata).workflow(WorkflowType.DELETION)
        assertNull("getStartedSnapshots should return null when deletion is null", builder.getStartedSnapshots())
    }

    fun `test sm metadata builder resetRetry with null creation retry`() {
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = SMMetadata.WorkflowMetadata(
                currentState = SMState.CREATION_START,
                trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
                retry = null,
            ),
            deletion = null,
        )
        val builder = SMMetadata.Builder(metadata).workflow(WorkflowType.CREATION)
        val result = builder.resetRetry()
        assertEquals("resetRetry should not change metadata when retry is null", metadata, result.build())
    }

    fun `test sm metadata builder resetRetry with null deletion retry`() {
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = null,
            deletion = SMMetadata.WorkflowMetadata(
                currentState = org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState.DELETION_START,
                trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
                retry = null,
            ),
        )
        val builder = SMMetadata.Builder(metadata).workflow(org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType.DELETION)
        val result = builder.resetRetry()
        assertEquals("resetRetry should not change metadata when retry is null", metadata, result.build())
    }

    fun `test sm metadata builder resetRetry with non-null creation retry`() {
        val originalRetry = SMMetadata.Retry(count = 3)
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = SMMetadata.WorkflowMetadata(
                currentState = org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState.CREATION_START,
                trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
                retry = originalRetry,
            ),
            deletion = null,
        )
        val builder = SMMetadata.Builder(metadata).workflow(org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType.CREATION)
        val result = builder.resetRetry()
        val builtMetadata = result.build()
        assertNull("resetRetry should set retry to null", builtMetadata.creation?.retry)
        assertEquals("resetRetry should preserve other creation fields", metadata.creation?.currentState, builtMetadata.creation?.currentState)
        assertEquals("resetRetry should preserve trigger", metadata.creation?.trigger, builtMetadata.creation?.trigger)
    }

    fun `test sm metadata builder resetRetry with non-null deletion retry`() {
        val originalRetry = SMMetadata.Retry(count = 2)
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = null,
            deletion = SMMetadata.WorkflowMetadata(
                currentState = org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState.DELETION_START,
                trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
                retry = originalRetry,
            ),
        )
        val builder = SMMetadata.Builder(metadata).workflow(org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType.DELETION)
        val result = builder.resetRetry()
        val builtMetadata = result.build()
        assertNull("resetRetry should set retry to null", builtMetadata.deletion?.retry)
        assertEquals("resetRetry should preserve other deletion fields", metadata.deletion?.currentState, builtMetadata.deletion?.currentState)
        assertEquals("resetRetry should preserve trigger", metadata.deletion?.trigger, builtMetadata.deletion?.trigger)
    }

    fun `test sm metadata builder setNextCreationTime with null creation`() {
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = null,
            deletion = null,
        )
        val builder = SMMetadata.Builder(metadata)
        val newTime = java.time.Instant.now().plusSeconds(3600)
        val result = builder.setNextCreationTime(newTime)
        val builtMetadata = result.build()
        assertNotNull("setNextCreationTime should create new creation when null", builtMetadata.creation)
        assertEquals("setNextCreationTime should set correct time", newTime, builtMetadata.creation?.trigger?.time)
        assertEquals("setNextCreationTime should set correct state", org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState.CREATION_START, builtMetadata.creation?.currentState)
    }

    fun `test sm metadata builder resetWorkflow with creation workflow should reset creation state`() {
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = SMMetadata.WorkflowMetadata(
                currentState = SMState.CREATION_FINISHED,
                trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
                started = listOf("snapshot-1"),
                retry = SMMetadata.Retry(count = 1),
            ),
            deletion = null,
        )

        val builtMetadata = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.CREATION)
            .resetWorkflow()
            .build()

        assertEquals("Creation state should be reset to CREATION_START", SMState.CREATION_START, builtMetadata.creation?.currentState)
        assertNull("Creation started should be reset to null", builtMetadata.creation?.started)
        assertNull("Creation retry should be reset to null", builtMetadata.creation?.retry)
    }

    fun `test sm metadata builder resetWorkflow with deletion workflow should reset deletion state`() {
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = null,
            deletion = SMMetadata.WorkflowMetadata(
                currentState = SMState.DELETION_FINISHED,
                trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
                started = listOf("snapshot-1"),
                retry = SMMetadata.Retry(count = 1),
            ),
        )

        val builtMetadata = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.DELETION)
            .resetWorkflow()
            .build()

        assertEquals("Deletion state should be reset to DELETION_START", SMState.DELETION_START, builtMetadata.deletion?.currentState)
        assertNull("Deletion started should be reset to null", builtMetadata.deletion?.started)
        assertNull("Deletion retry should be reset to null", builtMetadata.deletion?.retry)
    }

    fun `test sm metadata builder resetWorkflow with both workflows should reset both states`() {
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = SMMetadata.WorkflowMetadata(
                currentState = SMState.CREATION_FINISHED,
                trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
                started = listOf("snapshot-1"),
                retry = SMMetadata.Retry(count = 1),
            ),
            deletion = SMMetadata.WorkflowMetadata(
                currentState = SMState.DELETION_FINISHED,
                trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
                started = listOf("snapshot-2"),
                retry = SMMetadata.Retry(count = 1),
            ),
        )

        val builtMetadata = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.CREATION)
            .resetWorkflow()
            .build()

        assertEquals("Creation state should be reset to CREATION_START", SMState.CREATION_START, builtMetadata.creation?.currentState)
        assertNull("Creation started should be reset to null", builtMetadata.creation?.started)
        assertNull("Creation retry should be reset to null", builtMetadata.creation?.retry)
        // Deletion should remain unchanged since we're resetting creation workflow
        assertEquals("Deletion state should remain unchanged", SMState.DELETION_FINISHED, builtMetadata.deletion?.currentState)
        assertNotNull("Deletion started should remain unchanged", builtMetadata.deletion?.started)
        assertNotNull("Deletion retry should remain unchanged", builtMetadata.deletion?.retry)
    }

    fun `test sm metadata builder resetCreation should set creation to null`() {
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = SMMetadata.WorkflowMetadata(
                currentState = SMState.CREATION_START,
                trigger = SMMetadata.Trigger(time = java.time.Instant.now()),
            ),
            deletion = null,
        )
        val builder = SMMetadata.Builder(metadata)
        val result = builder.resetCreation()
        val builtMetadata = result.build()
        assertNull("resetCreation should set creation to null", builtMetadata.creation)
    }
}
