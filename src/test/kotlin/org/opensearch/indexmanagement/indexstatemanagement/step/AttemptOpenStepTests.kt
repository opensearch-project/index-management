/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import org.opensearch.test.OpenSearchTestCase

class AttemptOpenStepTests : OpenSearchTestCase() {

    /*private val clusterService: ClusterService = mock()

    fun `test open step sets step status to failed when not acknowledged`() {
        val openIndexResponse = OpenIndexResponse(false, false)
        val client = getClient(getAdminClient(getIndicesAdminClient(openIndexResponse, null)))

        runBlocking {
            val openActionConfig = OpenActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val attemptOpenStep = AttemptOpenStep(clusterService, client, openActionConfig, managedIndexMetaData)
            attemptOpenStep.execute()
            val updatedManagedIndexMetaData = attemptOpenStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test open step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val openActionConfig = OpenActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val attemptOpenStep = AttemptOpenStep(clusterService, client, openActionConfig, managedIndexMetaData)
            attemptOpenStep.execute()
            val updatedManagedIndexMetaData = attemptOpenStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test open step remote transport exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val openActionConfig = OpenActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val attemptOpenStep = AttemptOpenStep(clusterService, client, openActionConfig, managedIndexMetaData)
            attemptOpenStep.execute()
            val updatedManagedIndexMetaData = attemptOpenStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(openIndexResponse: OpenIndexResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (openIndexResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<OpenIndexResponse>>(1)
                if (openIndexResponse != null) listener.onResponse(openIndexResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).open(any(), any())
        }
    }*/
}
