/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.fielddomain

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.OpenSearchSecurityException
import org.opensearch.Version
import org.opensearch.action.admin.indices.fielddomain.PutIndexFieldDomainsAction
import org.opensearch.action.admin.indices.fielddomain.PutIndexFieldDomainsRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.index.fielddomain.DateRangeFieldDomain
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata
import org.opensearch.indexmanagement.indexstatemanagement.action.PublishFieldDomainsAction
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainConfig
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.fielddomain.ComputeFieldDomainsAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.fielddomain.ComputeFieldDomainsRequest
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.fielddomain.ComputeFieldDomainsResponse
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.Client
import java.time.Instant

class AttemptPublishFieldDomainsStepTests : OpenSearchTestCase() {
    private val threadContext = ThreadContext(Settings.EMPTY)
    private val client = mock<Client>()
    private val fields = listOf(FieldDomainConfig("@timestamp", DateRangeFieldDomain.TYPE))
    private val step = AttemptPublishFieldDomainsStep(PublishFieldDomainsAction(fields, 0))
    private val events = mutableListOf<String>()

    override fun setUp() {
        super.setUp()
        threadContext.putTransient("identity", "policy")
    }

    fun `test computation and publication use policy identity in order`() {
        configureClient()
        val result = executeStep()
        assertEquals(Step.StepStatus.COMPLETED, result.stepMetaData?.stepStatus)
        assertEquals(AttemptPublishFieldDomainsStep.getSuccessMessage(INDEX_NAME, 1), result.info?.get("message"))
        assertEquals(listOf("@timestamp"), result.info?.get("fields"))
        assertEquals(listOf("compute", "publish"), events)
        verify(client, never()).admin()
        verify(client, never()).search(any(), any())
    }

    fun `test denied computation stops before publication for empty and populated fields`() {
        for (empty in listOf(false, true)) {
            events.clear()
            configureClient(empty = empty, computeFailure = OpenSearchSecurityException("computation denied"))
            val result = executeStep()
            assertEquals(Step.StepStatus.FAILED, result.stepMetaData?.stepStatus)
            assertEquals("computation denied", result.info?.get("cause"))
            assertEquals(listOf("compute"), events)
        }
        verify(client, never()).admin()
        verify(client, never()).search(any(), any())
    }

    fun `test unavailable computation action fails closed`() {
        configureClient(computeFailure = IllegalStateException("action unavailable"))
        val result = executeStep()
        assertEquals(Step.StepStatus.FAILED, result.stepMetaData?.stepStatus)
        assertEquals("action unavailable", result.info?.get("cause"))
        assertEquals(listOf("compute"), events)
    }

    fun `test step waits for write block before computing finalized field domains`() {
        val result = executeStep(writeBlocked = false)
        assertEquals(Step.StepStatus.CONDITION_NOT_MET, result.stepMetaData?.stepStatus)
        assertEquals(AttemptPublishFieldDomainsStep.getWriteBlockRequiredMessage(INDEX_NAME), result.info?.get("message"))
        assertTrue(events.isEmpty())
        verify(client, never()).execute(any(), any(), any<ActionListener<ActionResponse>>())
    }

    fun `test step fails when current index uuid does not match managed index uuid`() {
        val result = executeStep(indexUUID = "replacement-uuid")
        assertEquals(Step.StepStatus.FAILED, result.stepMetaData?.stepStatus)
        assertEquals(INDEX_UUID, result.info?.get("expected_uuid"))
        assertEquals("replacement-uuid", result.info?.get("actual_uuid"))
        verify(client, never()).execute(any(), any(), any<ActionListener<ActionResponse>>())
    }

    fun `test authorized empty computation completes without publishing`() {
        configureClient(empty = true)
        val result = executeStep()
        assertEquals(Step.StepStatus.COMPLETED, result.stepMetaData?.stepStatus)
        assertEquals(AttemptPublishFieldDomainsStep.getNoDomainMessage(INDEX_NAME), result.info?.get("message"))
        assertEquals(listOf("compute"), events)
    }

    fun `test computation permission does not bypass publication authorization`() {
        configureClient(publishFailure = OpenSearchSecurityException("publication denied"))
        val result = executeStep()
        assertEquals(Step.StepStatus.FAILED, result.stepMetaData?.stepStatus)
        assertEquals("publication denied", result.info?.get("cause"))
        assertEquals(listOf("compute", "publish"), events)
    }

    fun `test step fails when publish response is not acknowledged`() {
        configureClient(acknowledged = false)
        val result = executeStep()
        assertEquals(Step.StepStatus.FAILED, result.stepMetaData?.stepStatus)
        assertEquals(AttemptPublishFieldDomainsStep.getFailedPublishMessage(INDEX_NAME), result.info?.get("message"))
    }

    fun `test step handles remote transport exception with non-exception cause`() {
        configureClient(publishFailure = RemoteTransportException("remote failure", AssertionError("remote fatal")))
        val result = executeStep()
        assertEquals(Step.StepStatus.FAILED, result.stepMetaData?.stepStatus)
        assertEquals("remote fatal", result.info?.get("cause"))
    }

    private fun configureClient(
        empty: Boolean = false,
        computeFailure: Exception? = null,
        publishFailure: Exception? = null,
        acknowledged: Boolean = true,
    ) {
        val domains = if (empty) emptyList() else listOf(DateRangeFieldDomain("@timestamp", 1L, 2L, true, "ism"))
        doAnswer { invocation ->
            assertEquals("policy", threadContext.getTransient<String>("identity"))
            val listener = invocation.getArgument<ActionListener<ActionResponse>>(2)
            when (invocation.getArgument<Any>(0)) {
                ComputeFieldDomainsAction.INSTANCE -> {
                    events.add("compute")
                    val request = invocation.getArgument<ComputeFieldDomainsRequest>(1)
                    assertEquals(INDEX_NAME, request.targetIndex.name)
                    assertEquals(INDEX_UUID, request.targetIndex.uuid)
                    assertEquals(fields, request.fields)
                    if (computeFailure != null) {
                        listener.onFailure(computeFailure)
                    } else {
                        listener.onResponse(ComputeFieldDomainsResponse(IndexFieldDomainMetadata.getInstance().toCustomData(domains)))
                    }
                }

                PutIndexFieldDomainsAction.INSTANCE -> {
                    events.add("publish")
                    val request = invocation.getArgument<PutIndexFieldDomainsRequest>(1)
                    assertEquals(INDEX_NAME, request.targetIndex().name)
                    assertEquals(INDEX_UUID, request.targetIndex().uuid)
                    assertEquals(IndexFieldDomainMetadata.getInstance().toCustomData(domains), request.fieldDomainCustomData())
                    if (publishFailure != null) listener.onFailure(publishFailure) else listener.onResponse(AcknowledgedResponse(acknowledged))
                }

                else -> fail("Unexpected action")
            }
            null
        }.whenever(client).execute(any(), any(), any<ActionListener<ActionResponse>>())
    }

    private fun executeStep(writeBlocked: Boolean = true, indexUUID: String = INDEX_UUID): ManagedIndexMetaData {
        val managed = ManagedIndexMetaData(
            INDEX_NAME, INDEX_UUID, "policy", null, null, null, null, null, null, null,
            ActionMetaData(PublishFieldDomainsAction.name, Instant.now().toEpochMilli(), 0, false, 1, null, null),
            null, null, null,
        )
        val index = IndexMetadata.builder(INDEX_NAME)
            .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, indexUUID).put(IndexMetadata.SETTING_BLOCKS_WRITE, writeBlocked))
            .numberOfShards(1).numberOfReplicas(0).build()
        val state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(index, false)).build()
        val context = StepContext(
            managed, mock { on { state() } doReturn state }, client, threadContext, null, mock(), Settings.EMPTY, mock(),
        )
        runBlocking { step.preExecute(logger, context).execute() }
        assertEquals("policy", threadContext.getTransient<String>("identity"))
        return step.getUpdatedManagedIndexMetadata(managed)
    }

    companion object {
        private const val INDEX_NAME = "test-index"
        private const val INDEX_UUID = "test-uuid"
    }
}
