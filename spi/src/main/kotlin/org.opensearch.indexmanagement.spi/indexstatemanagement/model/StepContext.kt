/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.model

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.transport.client.Client

class StepContext(
    val metadata: ManagedIndexMetaData,
    val clusterService: ClusterService,
    val client: Client,
    val threadContext: ThreadContext?,
    val user: User?,
    val scriptService: ScriptService,
    val settings: Settings,
    val lockService: LockService,
) {
    fun getUpdatedContext(metadata: ManagedIndexMetaData): StepContext = StepContext(metadata, this.clusterService, this.client, this.threadContext, this.user, this.scriptService, this.settings, this.lockService)
}
