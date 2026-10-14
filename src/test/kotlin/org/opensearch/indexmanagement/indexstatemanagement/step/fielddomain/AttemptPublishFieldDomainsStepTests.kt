/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.fielddomain

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.Version
import org.opensearch.action.admin.indices.fielddomain.PutIndexFieldDomainsAction
import org.opensearch.action.admin.indices.refresh.RefreshResponse
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.MappingMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.CheckedRunnable
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.identity.PluginSubject
import org.opensearch.index.fielddomain.DateRangeFieldDomain
import org.opensearch.index.fielddomain.FieldDomain
import org.opensearch.indexmanagement.indexstatemanagement.action.PublishFieldDomainsAction
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.DateRangeFieldDomainCalculator
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainCalculator
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainCalculatorRegistry
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainConfig
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainSearchContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.search.DocValueFormat
import org.opensearch.search.SearchHit
import org.opensearch.search.SearchHits
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import java.security.Principal
import java.time.Instant

class AttemptPublishFieldDomainsStepTests : OpenSearchTestCase() {
    fun `test refresh and publication use policy identity around plugin boundary searches`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(IDENTITY, POLICY)
        val subject = pluginSubject(threadContext)
        val step = AttemptPublishFieldDomainsStep(
            action(),
            FieldDomainCalculatorRegistry(listOf(DateRangeFieldDomainCalculator())),
            searchContextFactory = { FieldDomainSearchContext(it) { subject } },
        )
        val index = IndexMetadata.builder(indexMetadata(writeBlocked = true))
            .putMapping(MappingMetadata("_doc", mapOf("properties" to mapOf("@timestamp" to mapOf("type" to "date")))))
            .build()
        val events = mutableListOf<String>()
        val refresh = refreshResponse()
        val client = client(refresh, AcknowledgedResponse(true), null)
        val indicesClient = client.admin().indices()
        val responses = listOf(1L, 2L).map { bound ->
            val hit = SearchHit(0)
            hit.sortValues(arrayOf<Any>(bound), arrayOf(DocValueFormat.RAW))
            val hits = SearchHits(arrayOf(hit), null, 0.0F)
            mock<SearchResponse> {
                on { getHits() } doReturn hits
                on { shardFailures } doReturn ShardSearchFailure.EMPTY_ARRAY
            }
        }.iterator()

        doAnswer { invocation ->
            assertEquals("Refresh must use the policy identity", POLICY, threadContext.getTransient<String>(IDENTITY))
            events.add("refresh")
            invocation.getArgument<ActionListener<RefreshResponse>>(1).onResponse(refresh)
        }.whenever(indicesClient).refresh(any(), any())
        doAnswer { invocation ->
            assertEquals("Boundary searches must use the plugin identity", PLUGIN, threadContext.getTransient<String>(IDENTITY))
            events.add("search")
            invocation.getArgument<ActionListener<SearchResponse>>(1).onResponse(responses.next())
        }.whenever(client).search(any(), any())
        doAnswer { invocation ->
            assertEquals("Publication must use the policy identity", POLICY, threadContext.getTransient<String>(IDENTITY))
            assertSame(PutIndexFieldDomainsAction.INSTANCE, invocation.getArgument<Any>(0))
            events.add("publish")
            invocation.getArgument<ActionListener<AcknowledgedResponse>>(2).onResponse(AcknowledgedResponse(true))
        }.whenever(client).execute(any(), any(), any<ActionListener<AcknowledgedResponse>>())

        val metadata = executeStep(step, index, client, threadContext)
        assertEquals(Step.StepStatus.COMPLETED, metadata.stepMetaData?.stepStatus)
        assertEquals(listOf("refresh", "search", "search", "publish"), events)
        assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
    }

    fun `test non date calculator receives the same plugin search context`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(IDENTITY, POLICY)
        val subject = pluginSubject(threadContext)
        val calculator = object : FieldDomainCalculator {
            override val type: String = "test_type"

            override suspend fun calculate(
                context: FieldDomainSearchContext,
                indexMetadata: IndexMetadata,
                config: FieldDomainConfig,
                finalized: Boolean,
            ): FieldDomain? {
                assertEquals("tenant", config.field)
                context.search(SearchSourceBuilder().size(0))
                assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
                return null
            }
        }
        val step = AttemptPublishFieldDomainsStep(
            PublishFieldDomainsAction(listOf(FieldDomainConfig("tenant", calculator.type)), 0),
            FieldDomainCalculatorRegistry(listOf(calculator)),
            searchContextFactory = { FieldDomainSearchContext(it) { subject } },
        )
        val client = client(refreshResponse(), null, null)
        val response = mock<SearchResponse> { on { shardFailures } doReturn ShardSearchFailure.EMPTY_ARRAY }
        var searched = false
        doAnswer { invocation ->
            assertEquals(PLUGIN, threadContext.getTransient<String>(IDENTITY))
            searched = true
            invocation.getArgument<ActionListener<SearchResponse>>(1).onResponse(response)
        }.whenever(client).search(any(), any())

        val metadata = executeStep(step, indexMetadata(writeBlocked = true), client, threadContext)

        assertTrue(searched)
        assertEquals(Step.StepStatus.COMPLETED, metadata.stepMetaData?.stepStatus)
        assertEquals(AttemptPublishFieldDomainsStep.getNoDomainMessage(INDEX_NAME), metadata.info?.get("message"))
        assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
    }

    fun `test step waits for write block before publishing finalized field domains`() {
        val step = AttemptPublishFieldDomainsStep(
            PublishFieldDomainsAction(
                fields = listOf(FieldDomainConfig("@timestamp", DateRangeFieldDomain.TYPE)),
                index = 0,
            ),
        )

        val updatedManagedIndexMetaData = executeStep(step, indexMetadata(writeBlocked = false), mock<Client>())

        assertEquals(Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            AttemptPublishFieldDomainsStep.getWriteBlockRequiredMessage(INDEX_NAME),
            updatedManagedIndexMetaData.info?.get("message"),
        )
    }

    fun `test step fails when current index uuid does not match managed index uuid`() {
        val step = AttemptPublishFieldDomainsStep(action())

        val updatedManagedIndexMetaData = executeStep(
            step,
            indexMetadata(indexUUID = "new-index-uuid", writeBlocked = true),
            mock<Client>(),
        )

        assertEquals(Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            AttemptPublishFieldDomainsStep.getIndexUuidMismatchMessage(INDEX_NAME),
            updatedManagedIndexMetaData.info?.get("message"),
        )
        assertEquals(INDEX_UUID, updatedManagedIndexMetaData.info?.get("expected_uuid"))
        assertEquals("new-index-uuid", updatedManagedIndexMetaData.info?.get("actual_uuid"))
    }

    fun `test step completes when calculators return no field domains`() {
        val step = AttemptPublishFieldDomainsStep(
            action(),
            FieldDomainCalculatorRegistry(listOf(TestCalculator(domain = null))),
        )
        val client = client(refreshResponse = refreshResponse(), publishResponse = null, publishException = null)

        val updatedManagedIndexMetaData = executeStep(step, indexMetadata(writeBlocked = true), client)

        assertEquals(Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            AttemptPublishFieldDomainsStep.getNoDomainMessage(INDEX_NAME),
            updatedManagedIndexMetaData.info?.get("message"),
        )
    }

    fun `test step fails when publish response is not acknowledged`() {
        val step = AttemptPublishFieldDomainsStep(
            action(),
            FieldDomainCalculatorRegistry(listOf(TestCalculator(domain = domain()))),
        )
        val client = client(refreshResponse = refreshResponse(), publishResponse = AcknowledgedResponse(false), publishException = null)

        val updatedManagedIndexMetaData = executeStep(step, indexMetadata(writeBlocked = true), client)

        assertEquals(Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            AttemptPublishFieldDomainsStep.getFailedPublishMessage(INDEX_NAME),
            updatedManagedIndexMetaData.info?.get("message"),
        )
    }

    fun `test step completes when publish response is acknowledged`() {
        val step = AttemptPublishFieldDomainsStep(
            action(),
            FieldDomainCalculatorRegistry(listOf(TestCalculator(domain = domain()))),
        )
        val client = client(refreshResponse = refreshResponse(), publishResponse = AcknowledgedResponse(true), publishException = null)

        val updatedManagedIndexMetaData = executeStep(step, indexMetadata(writeBlocked = true), client)

        assertEquals(Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            AttemptPublishFieldDomainsStep.getSuccessMessage(INDEX_NAME, 1),
            updatedManagedIndexMetaData.info?.get("message"),
        )
        assertEquals(listOf("@timestamp"), updatedManagedIndexMetaData.info?.get("fields"))
    }

    fun `test step fails with cause when publish throws exception`() {
        val step = AttemptPublishFieldDomainsStep(
            action(),
            FieldDomainCalculatorRegistry(listOf(TestCalculator(domain = domain()))),
        )
        val client = client(
            refreshResponse = refreshResponse(),
            publishResponse = null,
            publishException = IllegalArgumentException("publish failed"),
        )

        val updatedManagedIndexMetaData = executeStep(step, indexMetadata(writeBlocked = true), client)

        assertEquals(Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            AttemptPublishFieldDomainsStep.getFailedPublishMessage(INDEX_NAME),
            updatedManagedIndexMetaData.info?.get("message"),
        )
        assertEquals("publish failed", updatedManagedIndexMetaData.info?.get("cause"))
    }

    fun `test step handles remote transport exception with non-exception cause`() {
        val step = AttemptPublishFieldDomainsStep(
            action(),
            FieldDomainCalculatorRegistry(listOf(TestCalculator(domain = domain()))),
        )
        val client = client(
            refreshResponse = refreshResponse(),
            publishResponse = null,
            publishException = RemoteTransportException("remote failure", AssertionError("remote fatal")),
        )

        val updatedManagedIndexMetaData = executeStep(step, indexMetadata(writeBlocked = true), client)

        assertEquals(Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            AttemptPublishFieldDomainsStep.getFailedPublishMessage(INDEX_NAME),
            updatedManagedIndexMetaData.info?.get("message"),
        )
        assertEquals("remote fatal", updatedManagedIndexMetaData.info?.get("cause"))
    }

    private fun executeStep(
        step: AttemptPublishFieldDomainsStep,
        indexMetadata: IndexMetadata,
        client: Client,
        threadContext: ThreadContext? = null,
    ): ManagedIndexMetaData {
        val managedIndexMetaData = managedIndexMetaData()
        val context = StepContext(
            managedIndexMetaData,
            clusterServiceWithIndex(indexMetadata),
            client,
            threadContext,
            null,
            mock<ScriptService>(),
            Settings.EMPTY,
            mock<LockService>(),
        )

        runBlocking {
            step.preExecute(logger, context).execute()
        }

        return step.getUpdatedManagedIndexMetadata(managedIndexMetaData)
    }

    private fun action(): PublishFieldDomainsAction = PublishFieldDomainsAction(
        fields = listOf(FieldDomainConfig("@timestamp", DateRangeFieldDomain.TYPE)),
        index = 0,
    )

    private fun pluginSubject(threadContext: ThreadContext): PluginSubject = object : PluginSubject {
        override fun getPrincipal(): Principal = Principal { PLUGIN }

        override fun <E : Exception> runAs(runnable: CheckedRunnable<E>) {
            threadContext.stashContext().use {
                threadContext.putTransient(IDENTITY, PLUGIN)
                runnable.run()
            }
        }
    }

    private fun domain(): DateRangeFieldDomain = DateRangeFieldDomain(
        "@timestamp",
        "1",
        "2",
        true,
        "ism",
        null,
        "milliseconds",
    )

    private fun managedIndexMetaData(): ManagedIndexMetaData = ManagedIndexMetaData(
        INDEX_NAME,
        INDEX_UUID,
        "policy_id",
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        ActionMetaData(PublishFieldDomainsAction.name, Instant.now().toEpochMilli(), 0, false, 1, null, null),
        null,
        null,
        null,
    )

    private fun client(
        refreshResponse: RefreshResponse,
        publishResponse: AcknowledgedResponse?,
        publishException: Exception?,
    ): Client {
        val adminClient = adminClient(refreshResponse)
        return mock {
            on { admin() } doReturn adminClient
            doAnswer { invocation ->
                val listener = invocation.getArgument<ActionListener<AcknowledgedResponse>>(2)
                when {
                    publishResponse != null -> listener.onResponse(publishResponse)
                    publishException != null -> listener.onFailure(publishException)
                    else -> listener.onFailure(IllegalStateException("publish should not be called"))
                }
            }.whenever(this.mock).execute(any(), any(), any<ActionListener<AcknowledgedResponse>>())
        }
    }

    private fun adminClient(refreshResponse: RefreshResponse): AdminClient {
        val indicesAdminClient = indicesAdminClient(refreshResponse)
        return mock {
            on { indices() } doReturn indicesAdminClient
        }
    }

    private fun indicesAdminClient(refreshResponse: RefreshResponse): IndicesAdminClient = mock {
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<RefreshResponse>>(1)
            listener.onResponse(refreshResponse)
        }.whenever(this.mock).refresh(any(), any())
    }

    private fun refreshResponse(): RefreshResponse = mock {
        on { failedShards } doReturn 0
    }

    private fun clusterServiceWithIndex(indexMetadata: IndexMetadata): ClusterService {
        val clusterMetadata = Metadata.builder().put(indexMetadata, false).build()
        val clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(clusterMetadata).build()
        return mock { on { state() } doReturn clusterState }
    }

    private fun indexMetadata(
        indexUUID: String = INDEX_UUID,
        writeBlocked: Boolean,
    ): IndexMetadata = IndexMetadata.Builder(INDEX_NAME)
        .settings(
            settings(Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                .put(IndexMetadata.SETTING_BLOCKS_WRITE, writeBlocked)
                .build(),
        )
        .numberOfShards(1)
        .numberOfReplicas(0)
        .build()

    companion object {
        private const val INDEX_NAME = "test-index"
        private const val INDEX_UUID = "test-index-uuid"
        private const val IDENTITY = "test_identity"
        private const val POLICY = "policy-user"
        private const val PLUGIN = "plugin:test"
    }

    private class TestCalculator(
        private val domain: FieldDomain?,
    ) : FieldDomainCalculator {
        override val type: String = DateRangeFieldDomain.TYPE

        override suspend fun calculate(
            context: FieldDomainSearchContext,
            indexMetadata: IndexMetadata,
            config: FieldDomainConfig,
            finalized: Boolean,
        ): FieldDomain? = domain
    }
}
