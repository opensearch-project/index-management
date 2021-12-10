/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import org.opensearch.test.OpenSearchTestCase

class SetReadWriteStepTests : OpenSearchTestCase() {

    /*private val clusterService: ClusterService = mock()

    fun `test read write step sets step status to failed when not acknowledged`() {
        val setReadWriteResponse = AcknowledgedResponse(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(setReadWriteResponse, null)))

        runBlocking {
            val readWriteActionConfig = ReadWriteActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val setReadWriteStep = SetReadWriteStep(clusterService, client, readWriteActionConfig, managedIndexMetaData)
            setReadWriteStep.execute()
            val updatedManagedIndexMetaData = setReadWriteStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test read write step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val readWriteActionConfig = ReadWriteActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val setReadWriteStep = SetReadWriteStep(clusterService, client, readWriteActionConfig, managedIndexMetaData)
            setReadWriteStep.execute()
            val updatedManagedIndexMetaData = setReadWriteStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test read write step sets step status to failed when remote transport error thrown`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val readWriteActionConfig = ReadWriteActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val setReadWriteStep = SetReadWriteStep(clusterService, client, readWriteActionConfig, managedIndexMetaData)
            setReadWriteStep.execute()
            val updatedManagedIndexMetaData = setReadWriteStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(setReadWriteResponse: AcknowledgedResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (setReadWriteResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
                if (setReadWriteResponse != null) listener.onResponse(setReadWriteResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).updateSettings(any(), any())
        }
    }*/
}
