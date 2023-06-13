/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:Suppress("ReturnCount")
package org.opensearch.indexmanagement

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.OpenSearchStatusException
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_REPLICAS
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.rest.RestStatus
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

@OpenForTesting
class IndexManagementIndices(
    settings: Settings,
    private val client: IndicesAdminClient,
    private val clusterService: ClusterService
) {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var historyNumberOfShards = ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS.get(settings)
    @Volatile private var historyNumberOfReplicas = ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS) {
            historyNumberOfShards = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS) {
            historyNumberOfReplicas = it
        }
    }

    fun checkAndUpdateIMConfigIndex(actionListener: ActionListener<AcknowledgedResponse>) {
        if (!indexManagementIndexExists()) {
            val indexRequest = CreateIndexRequest(INDEX_MANAGEMENT_INDEX)
                .mapping(indexManagementMappings)
                .settings(Settings.builder().put(INDEX_HIDDEN, true).build())
            client.create(
                indexRequest,
                object : ActionListener<CreateIndexResponse> {
                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(e)
                    }

                    override fun onResponse(response: CreateIndexResponse) {
                        actionListener.onResponse(response)
                    }
                }
            )
        } else {
            IndexUtils.checkAndUpdateConfigIndexMapping(clusterService.state(), client, actionListener)
        }
    }

    suspend fun checkAndUpdateIMConfigIndex(logger: Logger): Boolean {
        val response: AcknowledgedResponse = suspendCoroutine { cont ->
            checkAndUpdateIMConfigIndex(
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) = cont.resume(response)
                    override fun onFailure(e: Exception) = cont.resumeWithException(e)
                }
            )
        }
        if (response.isAcknowledged) {
            return true
        } else {
            logger.error("Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping.")
            throw OpenSearchStatusException(
                "Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping.",
                RestStatus.INTERNAL_SERVER_ERROR
            )
        }
    }

    fun indexManagementIndexExists(): Boolean = clusterService.state().routingTable.hasIndex(INDEX_MANAGEMENT_INDEX)

    /**
     * Attempt to create [INDEX_MANAGEMENT_INDEX] and return whether it exists
     */
    suspend fun attemptInitStateManagementIndex(client: Client): Boolean {
        if (indexManagementIndexExists()) return true

        return try {
            val response: AcknowledgedResponse = client.suspendUntil { checkAndUpdateIMConfigIndex(it) }
            if (response.isAcknowledged) {
                return true
            }
            logger.error("Creating $INDEX_MANAGEMENT_INDEX with mappings NOT acknowledged")
            return false
        } catch (e: ResourceAlreadyExistsException) {
            true
        } catch (e: Exception) {
            logger.error("Error trying to create $INDEX_MANAGEMENT_INDEX", e)
            false
        }
    }

    suspend fun attemptUpdateConfigIndexMapping(): Boolean {
        return try {
            val response: AcknowledgedResponse = client.suspendUntil {
                IndexUtils.checkAndUpdateConfigIndexMapping(clusterService.state(), client, it)
            }
            if (response.isAcknowledged) return true
            logger.error("Trying to update config index mapping not acknowledged.")
            return false
        } catch (e: Exception) {
            logger.error("Failed when trying to update config index mapping.", e)
            false
        }
    }

    /**
     * ============== History =============
     */
    fun indexStateManagementIndexHistoryExists(): Boolean = clusterService.state().metadata.hasAlias(HISTORY_WRITE_INDEX_ALIAS)

    @Suppress("ReturnCount")
    suspend fun checkAndUpdateHistoryIndex(): Boolean {
        if (!indexStateManagementIndexHistoryExists()) {
            return createHistoryIndex(HISTORY_INDEX_PATTERN, HISTORY_WRITE_INDEX_ALIAS)
        } else {
            val response: AcknowledgedResponse = client.suspendUntil {
                IndexUtils.checkAndUpdateHistoryIndexMapping(clusterService.state(), client, it)
            }
            if (response.isAcknowledged) {
                return true
            }
            logger.error("Updating $HISTORY_WRITE_INDEX_ALIAS with new mappings NOT acknowledged")
            return false
        }
    }

    private suspend fun createHistoryIndex(index: String, alias: String? = null): Boolean {
        // This should be a fast check of local cluster state. Should be exceedingly rare that the local cluster
        // state does not contain the index and multiple nodes concurrently try to create the index.
        // If it does happen that error is handled we catch the ResourceAlreadyExistsException
        val existsResponse: IndicesExistsResponse = client.suspendUntil {
            client.exists(IndicesExistsRequest(index).local(true), it)
        }
        if (existsResponse.isExists) return true

        val request = CreateIndexRequest(index)
            .mapping(indexStateManagementHistoryMappings)
            .settings(
                Settings.builder()
                    .put(INDEX_HIDDEN, true)
                    .put(INDEX_NUMBER_OF_SHARDS, historyNumberOfShards)
                    .put(INDEX_NUMBER_OF_REPLICAS, historyNumberOfReplicas).build()
            )
        if (alias != null) request.alias(Alias(alias))
        return try {
            val createIndexResponse: CreateIndexResponse = client.suspendUntil { client.create(request, it) }
            if (createIndexResponse.isAcknowledged) {
                true
            } else {
                logger.error("Creating $index with mappings NOT acknowledged")
                false
            }
        } catch (e: ResourceAlreadyExistsException) {
            true
        } catch (e: Exception) {
            logger.error("Error trying to create $index", e)
            false
        }
    }

    companion object {
        const val HISTORY_INDEX_BASE = ".opendistro-ism-managed-index-history"
        const val HISTORY_WRITE_INDEX_ALIAS = "$HISTORY_INDEX_BASE-write"
        const val HISTORY_INDEX_PATTERN = "<$HISTORY_INDEX_BASE-{now/d{yyyy.MM.dd}}-1>"
        const val HISTORY_ALL = "$HISTORY_INDEX_BASE*"

        val indexManagementMappings = IndexManagementIndices::class.java.classLoader
            .getResource("mappings/opendistro-ism-config.json").readText()
        val indexStateManagementHistoryMappings = IndexManagementIndices::class.java.classLoader
            .getResource("mappings/opendistro-ism-history.json").readText()
        val rollupTargetMappings = IndexManagementIndices::class.java.classLoader
            .getResource("mappings/opendistro-rollup-target.json").readText()
        val transformTargetMappings = IndexManagementIndices::class.java.classLoader
            .getResource("mappings/opendistro-transform-target.json").readText()
    }
}
