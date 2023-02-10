/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexmanagement.snapshotmanagement.settings

import org.opensearch.common.settings.Setting
import java.util.function.Function

@Suppress("UtilityClassWithPublicConstructor")
class SnapshotManagementSettings {

    companion object {
        private const val defaultMaxPoliciesPerRepo = 1
        const val defaultMaxSnapshotsPerPolicy = 400
        val defaultRepositoryDenyList = listOf("cs-", "_all", "\\*", "c\\*", "cs\\*")

        val FILTER_BY_BACKEND_ROLES: Setting<Boolean> = Setting.boolSetting(
            "plugins.snapshot_management.filter_by_backend_roles",
            false,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val MAXIMUM_POLICIES_PER_REPOSITORY: Setting<Int> = Setting.intSetting(
            "plugins.snapshot_management.maximum_policies_per_repository",
            defaultMaxPoliciesPerRepo,
            1,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        // -1 indicates this restriction is disabled
        val MAXIMUM_SNAPSHOTS_PER_POLICY: Setting<Int> = Setting.intSetting(
            "plugins.snapshot_management.maximum_snapshots_per_policy",
            defaultMaxSnapshotsPerPolicy,
            -1,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        // Any repository input starts with pattern in the denylist will be blocked
        val REPOSITORY_DENY_LIST: Setting<List<String>> = Setting.listSetting(
            "plugins.snapshot_management.repository_deny_list",
            defaultRepositoryDenyList,
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
