/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.monitor.jvm.JvmService

@OpenForTesting
class ValidateNothing(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService
) : Validate(settings, clusterService, jvmService) {

    // skips validation
    override fun execute(indexName: String): Validate {
        return this
    }
}
