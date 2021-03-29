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

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.action.SnapshotActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.SNAPSHOT_DENY_LIST
import org.opensearch.indexmanagement.indexstatemanagement.step.snapshot.AttemptSnapshotStep
import org.opensearch.indexmanagement.indexstatemanagement.step.snapshot.WaitForSnapshotStep
import org.opensearch.indexmanagement.waitFor
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class SnapshotActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test basic`() {
        val indexName = "${testIndexName}_index_basic"
        val policyID = "${testIndexName}_policy_basic"
        val repository = "repository"
        val snapshotText = "snapshot"
        val snapshot = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, snapshotText, emptyMap())
        val actionConfig = SnapshotActionConfig(repository, snapshot, 0)
        val states = listOf(
            State("Snapshot", listOf(actionConfig), listOf())
        )

        createRepository(repository)

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles for wait for snapshot step
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertSnapshotExists(repository, snapshotText) }
        waitFor { assertSnapshotFinishedWithSuccess(repository, snapshotText) }
    }

    fun `test basic with templated snapshot name`() {
        val indexName = "${testIndexName}_index_basic"
        val policyID = "${testIndexName}_policy_basic"
        val repository = "repository"
        val snapshot = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{{ctx.index}}", emptyMap())
        val actionConfig = SnapshotActionConfig(repository, snapshot, 0)
        val states = listOf(
            State("Snapshot", listOf(actionConfig), listOf())
        )

        createRepository(repository)

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles for wait for snapshot step
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertSnapshotExists(repository, indexName) }
        waitFor { assertSnapshotFinishedWithSuccess(repository, indexName) }
    }

    fun `test basic with invalid templated snapshot name default to indexName`() {
        val indexName = "${testIndexName}_index_basic"
        val policyID = "${testIndexName}_policy_basic"
        val repository = "repository"
        val snapshot = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{{ctx.someField}}", emptyMap())
        val actionConfig = SnapshotActionConfig(repository, snapshot, 0)
        val states = listOf(
            State("Snapshot", listOf(actionConfig), listOf())
        )

        createRepository(repository)

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles for wait for snapshot step
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertSnapshotExists(repository, indexName) }
        waitFor { assertSnapshotFinishedWithSuccess(repository, indexName) }
    }

    fun `test successful wait for snapshot step`() {
        val indexName = "${testIndexName}_index_success"
        val policyID = "${testIndexName}_policy_success"
        val repository = "repository"
        val snapshotText = "snapshot_success_test"
        val snapshot = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, snapshotText, emptyMap())
        val actionConfig = SnapshotActionConfig(repository, snapshot, 0)
        val states = listOf(
            State("Snapshot", listOf(actionConfig), listOf())
        )

        createRepository(repository)

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so attempt snapshot step with execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(AttemptSnapshotStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // Change the start time so wait for snapshot step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(WaitForSnapshotStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // verify we set snapshotName in action properties
        waitFor {
            assert(
                getExplainManagedIndexMetaData(indexName).actionMetaData?.actionProperties?.snapshotName?.contains(snapshotText) == true
            )
        }

        waitFor { assertSnapshotExists(repository, snapshotText) }
        waitFor { assertSnapshotFinishedWithSuccess(repository, snapshotText) }
    }

    fun `test failed wait for snapshot step`() {
        val indexName = "${testIndexName}_index_failed"
        val policyID = "${testIndexName}_policy_failed"
        val repository = "repository"
        val snapshotText = "snapshot_failed_test"
        val snapshot = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, snapshotText, emptyMap())
        val actionConfig = SnapshotActionConfig(repository, snapshot, 0)
        val states = listOf(
            State("Snapshot", listOf(actionConfig), listOf())
        )

        createRepository(repository)

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so attempt snapshot step with execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(AttemptSnapshotStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // Confirm successful snapshot creation
        waitFor { assertSnapshotExists(repository, snapshotText) }
        waitFor { assertSnapshotFinishedWithSuccess(repository, snapshotText) }

        // Delete the snapshot so wait for step will fail with missing snapshot exception
        val snapshotName = getExplainManagedIndexMetaData(indexName).actionMetaData?.actionProperties?.snapshotName
        assertNotNull("Snapshot name is null", snapshotName)
        deleteSnapshot(repository, snapshotName!!)

        // Change the start time so wait for snapshot step will execute where we should see a missing snapshot exception
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(WaitForSnapshotStep.getFailedMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message"))
            assertEquals("[$repository:$snapshotName] is missing", getExplainManagedIndexMetaData(indexName).info?.get("cause"))
        }
    }

    fun `test snapshot repository blocked`() {
        val denyList = listOf("hello-*")
        updateClusterSetting(SNAPSHOT_DENY_LIST.key, "hello-*")

        val indexName = "${testIndexName}_index_blocked"
        val policyID = "${testIndexName}_policy_basic"
        val repository = "hello-world"
        val snapshot = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "snapshot", emptyMap())
        val actionConfig = SnapshotActionConfig(repository, snapshot, 0)
        val states = listOf(
            State("Snapshot", listOf(actionConfig), listOf())
        )

        createRepository(repository)

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertEquals(AttemptSnapshotStep.getBlockedMessage(denyList, repository, indexName), getExplainManagedIndexMetaData(indexName).info?.get("message"))
        }
    }
}
