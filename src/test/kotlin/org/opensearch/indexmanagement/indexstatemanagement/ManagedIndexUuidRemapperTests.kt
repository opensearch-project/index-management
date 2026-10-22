/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.mockito.Mockito.verifyNoInteractions
import org.opensearch.Version
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.bulk.BulkItemResponse
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.get.GetResponse
import org.opensearch.action.get.MultiGetItemResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.index.get.GetResult
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.opensearchapi.contentParser
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.randomUser
import org.opensearch.indexmanagement.spi.indexstatemanagement.IndexMetadataService
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.search.DocValueFormat
import org.opensearch.search.SearchHit
import org.opensearch.search.SearchHits
import org.opensearch.snapshots.RestoreContext
import org.opensearch.snapshots.Snapshot
import org.opensearch.snapshots.SnapshotId
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.Client

class ManagedIndexUuidRemapperTests : OpenSearchTestCase() {
    private lateinit var client: Client
    private lateinit var ismIndices: IndexManagementIndices

    // What the fake config index holds: jobs by page, in the order the remapper reads them, and the jobs' metadata.
    private var jobPages: List<List<ManagedIndexConfig>> = emptyList()
    private var storedMetadata: List<ManagedIndexMetaData> = emptyList()
    private var failedReads: Set<String> = emptySet()
    private var failedWrites: Set<String> = emptySet()

    private val jobSearches = mutableListOf<SearchRequest>()
    private val metadataGets = mutableListOf<MultiGetRequest>()
    private val bulkRequests = mutableListOf<BulkRequest>()

    @Before
    fun setup() {
        ismIndices = mock()
        runBlocking { whenever(ismIndices.attemptUpdateConfigIndexMapping()).thenReturn(true) }
        client = mock()
        doAnswer { invocation ->
            val request = invocation.getArgument<SearchRequest>(0)
            invocation.getArgument<ActionListener<SearchResponse>>(1).onResponse(search(request))
        }.whenever(client).search(any(), any())
        doAnswer { invocation ->
            val request = invocation.getArgument<MultiGetRequest>(0)
            invocation.getArgument<ActionListener<MultiGetResponse>>(1).onResponse(multiGet(request))
        }.whenever(client).multiGet(any(), any())
        doAnswer { invocation ->
            val request = invocation.getArgument<BulkRequest>(0)
            invocation.getArgument<ActionListener<BulkResponse>>(1).onResponse(bulk(request))
        }.whenever(client).bulk(any(), any())
    }

    fun `test a restore without the config index changes nothing`() {
        remapper(index("abc", "456")).moveRestoredJobs(restoreOf("abc"))

        verifyNoInteractions(client, ismIndices)
    }

    fun `test the plugin hands a restore to the remapper`() {
        jobPages = listOf(listOf(job("abc", "123")))
        val plugin = IndexManagementPlugin().apply { managedIndexUuidRemapper = remapper(index("abc", "456")) }

        plugin.onRestore(restoreOf(INDEX_MANAGEMENT_INDEX))

        assertEquals(1, jobSearches.size)
    }

    fun `test a restored job and its metadata move to the new uuid of their index`() {
        val job = job("abc", "123")
        val metadata = metadata(job, CREATION_DATE)
        jobPages = listOf(listOf(job))
        storedMetadata = listOf(metadata)

        remapper(index("abc", "456")).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        val bulkRequest = bulkRequests.single()
        // The copies must be searchable before the coordinator's sweep looks for jobs.
        assertEquals(WriteRequest.RefreshPolicy.WAIT_UNTIL, bulkRequest.refreshPolicy)
        val (metadataWrite, jobWrite) = bulkRequest.requests().map { it as IndexRequest }
        assertEquals(managedIndexMetadataID("456"), metadataWrite.id())
        assertEquals("456", metadataWrite.routing())
        assertEquals(metadata.copy(indexUuid = "456"), parseMetadata(metadataWrite))
        assertEquals("456", jobWrite.id())
        assertEquals("456", jobWrite.routing())
        assertEquals(job.copy(indexUuid = "456"), parseJob(jobWrite))
        // Unconditional writes, so a copy replaces a job attached to the index from an ISM template.
        listOf(metadataWrite, jobWrite).forEach {
            assertEquals(DocWriteRequest.OpType.INDEX, it.opType())
            assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, it.ifSeqNo())
        }
    }

    fun `test a job is not moved to an index recreated under the same name`() {
        val job = job("abc", "123")
        jobPages = listOf(listOf(job))
        storedMetadata = listOf(metadata(job, CREATION_DATE))

        remapper(index("abc", "456", creationDate = CREATION_DATE + 1)).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        assertEquals(1, metadataGets.size)
        assertTrue(bulkRequests.isEmpty())
    }

    fun `test a job without metadata that has a creation date is not moved`() {
        val withoutMetadata = job("abc", "123")
        val withoutCreationDate = job("def", "234")
        jobPages = listOf(listOf(withoutMetadata, withoutCreationDate))
        storedMetadata = listOf(metadata(withoutCreationDate, creationDate = null))

        remapper(index("abc", "456"), index("def", "567")).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        assertEquals(1, metadataGets.size)
        assertTrue(bulkRequests.isEmpty())
    }

    fun `test a job whose metadata cannot be read is not moved`() {
        val unreadable = job("abc", "123")
        val readable = job("def", "234")
        jobPages = listOf(listOf(unreadable, readable))
        storedMetadata = listOf(metadata(unreadable, CREATION_DATE), metadata(readable, CREATION_DATE))
        failedReads = setOf(managedIndexMetadataID("123"))

        remapper(index("abc", "456"), index("def", "567")).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        assertEquals(listOf("567#metadata", "567"), bulkRequests.single().requests().map { it.id() })
    }

    fun `test a job whose index is not on this cluster is not moved`() {
        jobPages = listOf(listOf(job("abc", "123")))

        remapper(index("def", "456")).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        assertEquals(1, jobSearches.size)
        assertTrue(metadataGets.isEmpty())
        assertTrue(bulkRequests.isEmpty())
    }

    fun `test a job whose uuid matches an index is left alone`() {
        val job = job("abc", "456")
        jobPages = listOf(listOf(job))
        storedMetadata = listOf(metadata(job, CREATION_DATE))

        remapper(index("abc", "456")).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        assertTrue(metadataGets.isEmpty())
        assertTrue(bulkRequests.isEmpty())
    }

    fun `test the index uuid is read the same way as the runner reads it`() {
        // An index that keeps its ISM uuid in a custom setting still matches its job after a restore.
        val customUuidSetting = "index.custom_uuid"
        val job = job("abc", "123")
        jobPages = listOf(listOf(job))
        storedMetadata = listOf(metadata(job, CREATION_DATE))
        val index = index("abc", "456", settings = Settings.builder().put(customUuidSetting, "123").build())

        remapper(index, customUuidSetting = customUuidSetting).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        assertTrue(metadataGets.isEmpty())
        assertTrue(bulkRequests.isEmpty())
    }

    fun `test jobs are read in pages`() {
        val jobs = listOf(job("abc", "123"), job("def", "234"), job("ghi", "345"))
        jobPages = listOf(jobs.take(2), jobs.drop(2))
        storedMetadata = jobs.map { metadata(it, CREATION_DATE) }

        remapper(index("abc", "456"), index("def", "567"), index("ghi", "678"), pageSize = 2).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        assertEquals(2, jobSearches.size)
        assertNull(jobSearches[0].source().searchAfter())
        assertArrayEquals(arrayOf<Any>("234"), jobSearches[1].source().searchAfter())
        assertEquals(listOf(listOf("456#metadata", "456", "567#metadata", "567"), listOf("678#metadata", "678")), bulkRequests.map { bulk -> bulk.requests().map { it.id() } })
    }

    fun `test a full last page is followed by one empty page`() {
        val jobs = listOf(job("abc", "123"), job("def", "234"))
        jobPages = listOf(jobs)
        storedMetadata = jobs.map { metadata(it, CREATION_DATE) }

        remapper(index("abc", "456"), index("def", "567"), pageSize = 2).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        assertEquals(2, jobSearches.size)
        assertEquals(1, bulkRequests.size)
    }

    fun `test jobs are not moved when the config index mapping cannot be updated`() {
        runBlocking { whenever(ismIndices.attemptUpdateConfigIndexMapping()).thenReturn(false) }
        jobPages = listOf(listOf(job("abc", "123")))

        remapper(index("abc", "456")).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        verifyNoInteractions(client)
    }

    fun `test a failed write does not stop the other jobs from moving`() {
        val jobs = listOf(job("abc", "123"), job("def", "234"))
        jobPages = listOf(jobs)
        storedMetadata = jobs.map { metadata(it, CREATION_DATE) }
        failedWrites = setOf(managedIndexMetadataID("456"))

        remapper(index("abc", "456"), index("def", "567")).moveRestoredJobs(restoreOf(INDEX_MANAGEMENT_INDEX))

        assertEquals(listOf("456#metadata", "456", "567#metadata", "567"), bulkRequests.single().requests().map { it.id() })
    }

    private fun remapper(
        vararg indices: IndexMetadata,
        pageSize: Int = ManagedIndexUuidRemapper.PAGE_SIZE,
        customUuidSetting: String? = null,
    ): ManagedIndexUuidRemapper {
        val metadata = Metadata.builder()
        indices.forEach { metadata.put(it, false) }
        val state = ClusterState.builder(ClusterName("test")).metadata(metadata).build()
        val clusterSettings = ClusterSettings(Settings.EMPTY, setOf(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN))
        val clusterService: ClusterService =
            mock {
                on { state() } doReturn state
                on { getClusterSettings() } doReturn clusterSettings
            }
        val services = mutableMapOf<String, IndexMetadataService>(DEFAULT_INDEX_TYPE to DefaultIndexMetadataService(customUuidSetting))
        val indexMetadataProvider = IndexMetadataProvider(Settings.EMPTY, client, clusterService, services)
        return ManagedIndexUuidRemapper(client, clusterService, ismIndices, indexMetadataProvider, xContentRegistry(), pageSize)
    }

    private fun search(request: SearchRequest): SearchResponse {
        val query = request.source().query()
        if (query !is BoolQueryBuilder) throw AssertionError("unexpected search $query")
        jobSearches.add(request)
        val matches =
            jobPages.getOrElse(jobSearches.size - 1) { emptyList() }.map { job ->
                hit(job.indexUuid, job.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)).apply {
                    sortValues(arrayOf<Any>(job.indexUuid), arrayOf(DocValueFormat.RAW))
                }
            }
        return mock { on { hits } doReturn SearchHits(matches.toTypedArray(), null, 1.0f) }
    }

    private fun hit(id: String, source: XContentBuilder): SearchHit = SearchHit(0, id, emptyMap(), emptyMap()).sourceRef(BytesReference.bytes(source))

    private fun multiGet(request: MultiGetRequest): MultiGetResponse {
        metadataGets.add(request)
        val items =
            request.items.map { item ->
                val metadata = storedMetadata.find { managedIndexMetadataID(it.indexUuid) == item.id() }
                when {
                    item.id() in failedReads ->
                        MultiGetItemResponse(null, MultiGetResponse.Failure(INDEX_MANAGEMENT_INDEX, item.id(), IllegalStateException("unavailable")))

                    metadata == null -> MultiGetItemResponse(GetResponse(missing(item.id())), null)

                    else -> {
                        val source = BytesReference.bytes(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS, true))
                        MultiGetItemResponse(GetResponse(GetResult(INDEX_MANAGEMENT_INDEX, item.id(), 1, 1, 1, true, source, null, null)), null)
                    }
                }
            }
        return MultiGetResponse(items.toTypedArray())
    }

    private fun missing(id: String) = GetResult(
        INDEX_MANAGEMENT_INDEX, id, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null,
    )

    private fun bulk(request: BulkRequest): BulkResponse {
        bulkRequests.add(request)
        val items =
            request.requests().mapIndexed { item, write ->
                if (write.id() in failedWrites) {
                    BulkItemResponse(item, write.opType(), BulkItemResponse.Failure(INDEX_MANAGEMENT_INDEX, write.id(), IllegalStateException("rejected")))
                } else {
                    BulkItemResponse(item, write.opType(), IndexResponse(ShardId(INDEX_MANAGEMENT_INDEX, "config-uuid", 0), write.id(), 1, 1, 1, true))
                }
            }
        return BulkResponse(items.toTypedArray(), 1)
    }

    private fun parseJob(write: IndexRequest): ManagedIndexConfig = contentParser(write.source()).parseWithType(parse = ManagedIndexConfig.Companion::parse)

    private fun parseMetadata(write: IndexRequest): ManagedIndexMetaData = ManagedIndexMetaData.parseWithType(contentParser(write.source()))

    private fun job(index: String, uuid: String): ManagedIndexConfig {
        // ISM runs the policy's actions with the roles of the user saved in the job's copy of the policy.
        val policy = randomPolicy(states = listOf(randomState())).copy(user = randomUser())
        return randomManagedIndexConfig(index = index, uuid = uuid, policy = policy, changePolicy = null)
    }

    private fun metadata(job: ManagedIndexConfig, creationDate: Long?) = ManagedIndexMetaData(
        index = job.index,
        indexUuid = job.indexUuid,
        policyID = job.policyID,
        policySeqNo = job.policySeqNo,
        policyPrimaryTerm = job.policyPrimaryTerm,
        policyCompleted = false,
        rolledOver = false,
        indexCreationDate = creationDate,
        transitionTo = null,
        stateMetaData = StateMetaData("warm", CREATION_DATE + 60_000),
        actionMetaData = null,
        stepMetaData = null,
        policyRetryInfo = PolicyRetryInfoMetaData(failed = false, consumedRetries = 0),
        info = mapOf("message" to "Transitioning to warm"),
    )

    private fun index(name: String, uuid: String, creationDate: Long = CREATION_DATE, settings: Settings = Settings.EMPTY): IndexMetadata = IndexMetadata.builder(name)
        .settings(Settings.builder().put(settings).put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, uuid))
        .creationDate(creationDate)
        .numberOfShards(1)
        .numberOfReplicas(0)
        .build()

    private fun restoreOf(vararg indices: String) = RestoreContext(
        Snapshot("repo", SnapshotId("snap", "snap-uuid")),
        "restore-uuid",
        indices.map { Index(it, "$it-uuid") },
        false,
    )

    companion object {
        private const val CREATION_DATE = 1_700_000_000_000L
    }
}
