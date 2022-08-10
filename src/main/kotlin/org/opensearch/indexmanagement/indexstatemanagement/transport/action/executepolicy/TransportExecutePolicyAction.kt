/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.executepolicy

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.node.NodeClient
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.ExceptionsHelper
import org.opensearch.index.fielddata.IndexFieldDataCache.None
import org.opensearch.jobscheduler.spi.JobDocVersion
import org.opensearch.jobscheduler.spi.JobExecutionContext
import java.time.Instant

private val log = LogManager.getLogger(TransportExecutePolicyAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportExecutePolicyAction @Inject constructor(
        transportService: TransportService,
        val client: NodeClient,
        private val clusterService: ClusterService,
        private val runner: ManagedIndexRunner,
        actionFilters: ActionFilters,
        val xContentRegistry: NamedXContentRegistry,
        private val settings: Settings
) : HandledTransportAction<ExecutePolicyRequest, AcknowledgedResponse> (
    ExecutePolicyAction.NAME, transportService, actionFilters, ::ExecutePolicyRequest
) {
    override fun doExecute(task: Task, execPolicyRequest: ExecutePolicyRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        runner.launch {
            try {
                val lock = LockService(client, clusterService)
                // fake context in order to pass into runJob
                val newContext = JobExecutionContext(Instant.now(), JobDocVersion(0L,0L,0L), lock,
                        "", "")
                runner.runJob(None, newContext)
            } catch (e: Exception) {
                log.error("Unexpected error trying to execute policy")
                withContext(Dispatchers.IO) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            }
        }
    }
}