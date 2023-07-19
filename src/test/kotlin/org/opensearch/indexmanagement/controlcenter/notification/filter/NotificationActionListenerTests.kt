/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter

import org.junit.Assert
import org.junit.Before
import org.mockito.Mockito.mock
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.controlcenter.notification.action.get.GetLRONConfigResponse
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONCondition
import org.opensearch.indexmanagement.controlcenter.notification.randomLRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.randomLRONConfigResponse
import org.opensearch.tasks.Task
import org.opensearch.test.OpenSearchTestCase

class NotificationActionListenerTests : OpenSearchTestCase() {

    private lateinit var listener: NotificationActionListener<ActionRequest, ActionResponse>
    private lateinit var delegate: ActionListener<ActionResponse>
    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var task: Task
    private lateinit var activeShardsObserver: ActiveShardsObserver
    private lateinit var request: ActionRequest
    private lateinit var indexNameExpressionResolver: IndexNameExpressionResolver

    @Before
    fun setup() {
        client = mock()
        delegate = mock()
        clusterService = mock()
        task = mock()
        activeShardsObserver = mock()
        indexNameExpressionResolver = mock()
        request = mock()
        listener = NotificationActionListener(
            delegate,
            client,
            clusterService,
            "open",
            task,
            activeShardsObserver,
            request,
            indexNameExpressionResolver
        )
    }

    fun `test all conditions are disabled`() {
        val lronConfig = randomLRONConfig(lronCondition = LRONCondition(false, false))
        val lronConfigResponse = randomLRONConfigResponse(lronConfig = lronConfig)
        val responses = GetLRONConfigResponse(
            lronConfigResponses = listOf(lronConfigResponse), 1
        )

        Assert.assertTrue(listener.getNotificationPolices(responses, OperationResult.COMPLETE).isEmpty())
        Assert.assertTrue(listener.getNotificationPolices(responses, OperationResult.FAILED).isEmpty())
    }

    fun `test success and failed conditions`() {
        val lronConfigWithTaskId = randomLRONConfig(lronCondition = LRONCondition(true, false))
        val lronConfigDefault = randomLRONConfig(
            lronCondition = LRONCondition(true, true),
            taskId = null,
            actionName = lronConfigWithTaskId.actionName
        )
        val lronConfigResponseWithTaskId = randomLRONConfigResponse(lronConfig = lronConfigWithTaskId)
        val lronConfigResponseDefault = randomLRONConfigResponse(lronConfig = lronConfigDefault)
        val responses = GetLRONConfigResponse(
            lronConfigResponses = listOf(lronConfigResponseWithTaskId, lronConfigResponseDefault), 2
        )

        Assert.assertEquals(2, listener.getNotificationPolices(responses, OperationResult.COMPLETE).size)
        Assert.assertEquals(1, listener.getNotificationPolices(responses, OperationResult.FAILED).size)
    }
}
