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
package org.opensearch.indexmanagement.settings

import org.opensearch.common.settings.Setting

class IndexManagementSettings {

    companion object {

        val FILTER_BY_BACKEND_ROLES: Setting<Boolean> = Setting.boolSetting(
            "plugins.index_management.filter_by_backend_roles",
            false,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
