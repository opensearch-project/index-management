/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import kotlinx.coroutines.runBlocking
import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM
import org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.buildMgetMetadataRequest
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.mgetResponseToMap
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.getSweptManagedIndexSearchRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexConfigIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataIndexRequest
import org.opensearch.indexmanagement.opensearchapi.parseFromSearchResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.search.sort.SortOrder
import org.opensearch.snapshots.RestoreContext
import org.opensearch.snapshots.Snapshot
import org.opensearch.transport.client.Client

/**
 * Moves the ISM jobs that a snapshot restore brings back to the new UUIDs of the indices they manage.
 *
 * A job (managed index config) and its metadata (the job's progress) are stored under the UUID of the index they
 * manage, and a restore gives every index a new UUID. So the jobs in a restored [INDEX_MANAGEMENT_INDEX] point at UUIDs
 * that no index has: their runs stop, and the coordinator's sweep deletes them. An index attached to its policy by an
 * ISM template then starts the policy over, and an index attached with the add policy API is left unmanaged.
 *
 * When a restore brings back [INDEX_MANAGEMENT_INDEX], this copies each such job and its metadata to the new UUID of the
 * index it belongs to, so the index carries on from the position saved in the snapshot. A job belongs to the index that
 * has the job's index name and the creation date saved in the job's metadata: a restore keeps an index's creation date,
 * while an index deleted and recreated under the same name gets a new one. The old job is left for the sweep to delete.
 *
 * Only indices already on this cluster can be matched, so the data indices must be restored, and their restore
 * finished, before [INDEX_MANAGEMENT_INDEX] is restored.
 */
class ManagedIndexUuidRemapper(
    private val client: Client,
    private val clusterService: ClusterService,
    private val ismIndices: IndexManagementIndices,
    indexMetadataProvider: IndexMetadataProvider,
    private val xContentRegistry: NamedXContentRegistry,
    private val pageSize: Int = PAGE_SIZE,
) {
    private val logger = LogManager.getLogger(javaClass)

    // The job runner reads an index's UUID the same way: it can come from a custom setting.
    private val indexMetadataService = indexMetadataProvider.services[DEFAULT_INDEX_TYPE] as DefaultIndexMetadataService

    /**
     * Core calls this in a task of its own on the generic thread pool, so blocking that thread until the jobs are moved
     * delays neither cluster-state updates nor other plugins. An exception reaches core, which logs it with the restore.
     */
    fun moveRestoredJobs(context: RestoreContext) {
        if (context.restoredIndices().none { it.name == INDEX_MANAGEMENT_INDEX }) return
        runBlocking { moveUnmatchedJobs(context.snapshot()) }
    }

    private suspend fun moveUnmatchedJobs(snapshot: Snapshot) {
        // A snapshot taken on an older ISM version brings back an older mapping, which can reject fields of the copies.
        if (!ismIndices.attemptUpdateConfigIndexMapping()) {
            logger.error("Not moving the ISM jobs restored from snapshot [$snapshot], failed to update the $INDEX_MANAGEMENT_INDEX mapping")
            return
        }
        val indices = clusterService.state().metadata.indices
        val indexUuids = indices.values.map { indexMetadataService.getIndexUUID(it) }.toSet()
        var searchAfter: Array<Any>? = null
        do {
            val response = searchJobPage(searchAfter)
            val jobs = parseFromSearchResponse(response, xContentRegistry, ManagedIndexConfig.Companion::parse)
            moveJobs(jobs, indices, indexUuids)
            val hits = response.hits.hits
            searchAfter = hits.lastOrNull()?.sortValues
        } while (hits.size == pageSize)
    }

    /**
     * Returns the [pageSize] jobs in [INDEX_MANAGEMENT_INDEX] that come after the job with the sort values [searchAfter],
     * or the first [pageSize] jobs if [searchAfter] is null. The jobs are found with the search the coordinator's sweep uses.
     */
    private suspend fun searchJobPage(searchAfter: Array<Any>?): SearchResponse {
        val request = getSweptManagedIndexSearchRequest(size = pageSize)
        // Sorted by job UUID, so each page starts after the last job of the page before it.
        request.source().sort(JOB_INDEX_UUID_FIELD, SortOrder.ASC)
        searchAfter?.let { request.source().searchAfter(it) }
        return client.suspendUntil { search(request, it) }
    }

    /**
     * Copies each job in [jobs] that has a UUID no index on this cluster has to the new UUID of the index it belongs to.
     */
    private suspend fun moveJobs(jobs: List<ManagedIndexConfig>, indices: Map<String, IndexMetadata>, indexUuids: Set<String>) {
        // A job to move has a UUID that no index on this cluster has, and the name of an index that is on it.
        val jobsToMove = jobs.filter { it.indexUuid !in indexUuids && it.index in indices }
        val jobsToMoveMetadata = readMetadata(jobsToMove)
        // Wait until the copies are searchable: the sweep finds jobs with a search, and deletes the metadata of an
        // index whose job that search does not return.
        val copies = BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
        for (job in jobsToMove) {
            val index = indices.getValue(job.index)
            val metadata = metadataToCopy(job, jobsToMoveMetadata[managedIndexMetadataID(job.indexUuid)], index) ?: continue
            copies.addCopy(job, metadata, indexMetadataService.getIndexUUID(index))
        }
        writeCopies(copies)
    }

    /**
     * Returns the metadata to copy if [job] belongs to [index], which it does when the index was created at the time
     * saved in the job's metadata. Otherwise logs why the job is not moved and returns null. [read] is the result of
     * reading the job's metadata: the metadata or the failure to read it, or null if the snapshot has no metadata for it.
     */
    private fun metadataToCopy(
        job: ManagedIndexConfig,
        read: Pair<ManagedIndexMetaData?, Exception?>?,
        index: IndexMetadata,
    ): ManagedIndexMetaData? {
        val (metadata, readFailure) = read ?: (null to null)
        when {
            readFailure != null ->
                logger.warn("Not moving the restored ISM job of index [${job.index}], failed to read its metadata", readFailure)

            metadata?.indexCreationDate != index.creationDate ->
                logger.info("Not moving the restored ISM job of index [${job.index}], its metadata has no creation date that matches the index on this cluster")

            else -> return metadata
        }
        return null
    }

    /**
     * Reads the metadata of [jobs] with the multi-get the change policy and retry APIs use. Returns, by metadata document
     * id, the metadata or the failure to read it.
     */
    private suspend fun readMetadata(jobs: List<ManagedIndexConfig>): Map<String, Pair<ManagedIndexMetaData?, Exception?>?> {
        if (jobs.isEmpty()) return emptyMap()
        val response: MultiGetResponse = client.suspendUntil { multiGet(buildMgetMetadataRequest(jobs.map { it.indexUuid }), it) }
        return mgetResponseToMap(response)
    }

    /**
     * Adds the writes that copy [job] and its [metadata] to [indexUuid]. The metadata is written first, so the copied
     * job's first run finds it instead of starting the policy over. The writes are not conditional: the sequence numbers
     * read with the old documents do not apply to the copies, and a copy replaces any job the coordinator attached to
     * the index from an ISM template in the meantime.
     */
    private fun BulkRequest.addCopy(job: ManagedIndexConfig, metadata: ManagedIndexMetaData, indexUuid: String) {
        val metadataCopy = metadata.copy(indexUuid = indexUuid, seqNo = UNASSIGNED_SEQ_NO, primaryTerm = UNASSIGNED_PRIMARY_TERM)
        val jobCopy = job.copy(indexUuid = indexUuid, seqNo = UNASSIGNED_SEQ_NO, primaryTerm = UNASSIGNED_PRIMARY_TERM)
        // The bulk request sets the refresh policy. A write inside a bulk request is not allowed to set its own.
        add(managedIndexMetadataIndexRequest(metadataCopy, waitRefresh = false))
        add(managedIndexConfigIndexRequest(jobCopy))
    }

    /** Writes the copies and logs each write that failed. A failed write does not stop the others. */
    private suspend fun writeCopies(copies: BulkRequest) {
        if (copies.numberOfActions() == 0) return
        val response: BulkResponse = client.suspendUntil { bulk(copies, it) }
        response.items.filter { it.isFailed }.forEach {
            logger.warn("Failed to write [${it.id}] while moving a restored ISM job to its index's new UUID: ${it.failureMessage}")
        }
    }

    companion object {
        // Each job holds a copy of its policy, so jobs are read in pages to bound memory.
        const val PAGE_SIZE = 1000
        private const val JOB_INDEX_UUID_FIELD = "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_UUID_FIELD}"
    }
}
