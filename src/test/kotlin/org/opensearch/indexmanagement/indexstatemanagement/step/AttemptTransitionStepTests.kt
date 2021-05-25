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

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.stats.CommonStats
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.shard.DocsStats
import org.opensearch.indexmanagement.indexstatemanagement.model.Conditions
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.model.action.TransitionsActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.transition.AttemptTransitionStep
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

class AttemptTransitionStepTests : OpenSearchTestCase() {

    private val indexMetadata: IndexMetadata = mock()
    private val metadata: Metadata = mock { on { index(any<String>()) } doReturn indexMetadata }
    private val clusterState: ClusterState = mock { on { metadata() } doReturn metadata }
    private val clusterService: ClusterService = mock { on { state() } doReturn clusterState }

    private val docsStats: DocsStats = mock()
    private val primaries: CommonStats = mock { on { getDocs() } doReturn docsStats }
    private val statsResponse: IndicesStatsResponse = mock { on { primaries } doReturn primaries }

    fun `test stats response not OK`() {
        whenever(indexMetadata.creationDate).doReturn(5L)
        whenever(statsResponse.status).doReturn(RestStatus.INTERNAL_SERVER_ERROR)
        whenever(statsResponse.shardFailures).doReturn(IndicesStatsResponse.EMPTY)
        whenever(docsStats.count).doReturn(6L)
        whenever(docsStats.totalSizeInBytes).doReturn(2)
        val client = getClient(getAdminClient(getIndicesAdminClient(statsResponse, null)))

        runBlocking {
            val config = TransitionsActionConfig(listOf(Transition("some_state", Conditions(docCount = 5L))))
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val step = AttemptTransitionStep(clusterService, client, config, managedIndexMetaData)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get correct failed message", AttemptTransitionStep.getFailedStatsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test transitions fails on exception`() {
        whenever(indexMetadata.creationDate).doReturn(5L)
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val config = TransitionsActionConfig(listOf(Transition("some_state", Conditions(docCount = 5L))))
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val step = AttemptTransitionStep(clusterService, client, config, managedIndexMetaData)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "example", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test transitions remote transport exception`() {
        whenever(indexMetadata.creationDate).doReturn(5L)
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val config = TransitionsActionConfig(listOf(Transition("some_state", Conditions(docCount = 5L))))
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val step = AttemptTransitionStep(clusterService, client, config, managedIndexMetaData)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test step start time resetting between two transitions`() {
        val client = getClient(getAdminClient(getIndicesAdminClient(statsResponse, null)))

        runBlocking {
            val config = TransitionsActionConfig(listOf(Transition("some_state", null)))
            val completedStartTime = Instant.now()
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, StepMetaData("attempt_transition", completedStartTime.toEpochMilli(), Step.StepStatus.COMPLETED), null, null)
            val step = AttemptTransitionStep(clusterService, client, config, managedIndexMetaData)
            Thread.sleep(50) // Make sure we give enough time for the instants to be different
            val startTime = step.getStepStartTime()
            assertNotEquals("Two separate transitions should not have same start time", completedStartTime.toEpochMilli(), startTime.toEpochMilli())
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(statsResponse: IndicesStatsResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (statsResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<IndicesStatsResponse>>(1)
                if (statsResponse != null) listener.onResponse(statsResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).stats(any(), any())
        }
    }
}
