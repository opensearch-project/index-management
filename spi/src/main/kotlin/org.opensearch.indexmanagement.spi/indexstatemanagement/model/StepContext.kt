/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.model

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.script.ScriptService

// TODO: Add more attributes here if needed
class StepContext(
    val metadata: ManagedIndexMetaData,
    val clusterService: ClusterService,
    val client: Client,
    val threadContext: ThreadContext?,
    val user: User?,
    val scriptService: ScriptService,
    val settings: Settings,
)
