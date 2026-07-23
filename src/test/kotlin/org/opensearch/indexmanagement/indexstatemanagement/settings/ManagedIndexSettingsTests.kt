/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.settings

import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.AliasAction
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction
import org.opensearch.indexmanagement.indexstatemanagement.action.CloseAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ForceMergeAction
import org.opensearch.indexmanagement.indexstatemanagement.action.IndexPriorityAction
import org.opensearch.indexmanagement.indexstatemanagement.action.NotificationAction
import org.opensearch.indexmanagement.indexstatemanagement.action.OpenAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadOnlyAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadWriteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ReplicaCountAction
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.action.RollupAction
import org.opensearch.indexmanagement.indexstatemanagement.action.SearchOnlyAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction
import org.opensearch.indexmanagement.indexstatemanagement.action.StopReplicationAction
import org.opensearch.indexmanagement.indexstatemanagement.action.TransformAction
import org.opensearch.indexmanagement.indexstatemanagement.action.TransitionsAction
import org.opensearch.test.OpenSearchTestCase

class ManagedIndexSettingsTests : OpenSearchTestCase() {
    fun `test allow running on red cluster defaults to false`() {
        assertEquals(false, ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER.get(Settings.EMPTY))
        assertEquals(false, ManagedIndexSettings.DEFAULT_ALLOW_RUNNING_ON_RED_CLUSTER)
    }

    fun `test allow running on red cluster can be enabled`() {
        val settings =
            Settings.builder()
                .put("plugins.index_state_management.allow_running_on_red_cluster", true)
                .build()
        assertEquals(true, ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER.get(settings))
    }

    fun `test allow running on red cluster is a dynamic node scope setting`() {
        val properties = ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER.properties
        assertTrue(
            "allow_running_on_red_cluster should have node scope",
            properties.contains(Setting.Property.NodeScope),
        )
        assertTrue(
            "allow_running_on_red_cluster should be dynamic",
            properties.contains(Setting.Property.Dynamic),
        )
    }

    fun `test recovery oriented actions are allowed on red cluster`() {
        // A null action (nothing to execute) should be treated as allowed.
        assertTrue(ManagedIndexSettings.isActionAllowedOnRedCluster(null))
        val allowed =
            listOf(
                DeleteAction.name,
                CloseAction.name,
                ReadOnlyAction.name,
                NotificationAction.name,
                IndexPriorityAction.name,
                AliasAction.name,
                StopReplicationAction.name,
                SearchOnlyAction.name,
                TransitionsAction.name,
            )
        allowed.forEach { action ->
            assertTrue("$action should be allowed on a red cluster", ManagedIndexSettings.isActionAllowedOnRedCluster(action))
            assertTrue("$action should be in the allowed set", ManagedIndexSettings.RED_CLUSTER_ALLOWED_ACTIONS.contains(action))
        }
    }

    fun `test unsafe actions are restricted on red cluster`() {
        val restricted =
            listOf(
                ReadWriteAction.name,
                ForceMergeAction.name,
                ReplicaCountAction.name,
                ShrinkAction.name,
                SnapshotAction.name,
                RolloverAction.name,
                OpenAction.name,
                AllocationAction.name,
                ConvertIndexToRemoteAction.name,
                TransformAction.name,
                RollupAction.name,
            )
        restricted.forEach { action ->
            assertFalse("$action should be restricted on a red cluster", ManagedIndexSettings.isActionAllowedOnRedCluster(action))
            assertFalse("$action should not be in the allowed set", ManagedIndexSettings.RED_CLUSTER_ALLOWED_ACTIONS.contains(action))
        }
    }

    fun `test unknown or new actions are restricted on red cluster by default`() {
        // Anything not explicitly allow-listed must default to restricted so that a newly added
        // action is never accidentally permitted on a red cluster before it has been reviewed.
        assertFalse(ManagedIndexSettings.isActionAllowedOnRedCluster("some_new_unreviewed_action"))
        assertTrue(ManagedIndexSettings.RED_CLUSTER_ALLOWED_ACTIONS.contains(DeleteAction.name))
    }
}
