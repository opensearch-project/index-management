/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.settings

import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction
import org.opensearch.indexmanagement.indexstatemanagement.action.CloseAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ForceMergeAction
import org.opensearch.indexmanagement.indexstatemanagement.action.NotificationAction
import org.opensearch.indexmanagement.indexstatemanagement.action.OpenAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadOnlyAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ReplicaCountAction
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.action.RollupAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction
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
        // Actions that help reclaim resources or are lightweight should be allowed.
        assertTrue(ManagedIndexSettings.isActionAllowedOnRedCluster(DeleteAction.name))
        assertTrue(ManagedIndexSettings.isActionAllowedOnRedCluster(ReadOnlyAction.name))
        assertTrue(ManagedIndexSettings.isActionAllowedOnRedCluster(CloseAction.name))
        assertTrue(ManagedIndexSettings.isActionAllowedOnRedCluster(NotificationAction.name))
        assertTrue(ManagedIndexSettings.isActionAllowedOnRedCluster(TransitionsAction.name))
    }

    fun `test resource intensive actions are restricted on red cluster`() {
        val restricted =
            listOf(
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
            assertTrue("$action should be in the restricted set", ManagedIndexSettings.RED_CLUSTER_RESTRICTED_ACTIONS.contains(action))
        }
        // The delete action must never be part of the restricted set so recovery can proceed.
        assertFalse(ManagedIndexSettings.RED_CLUSTER_RESTRICTED_ACTIONS.contains(DeleteAction.name))
    }
}
