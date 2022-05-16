/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexAbstraction
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.hash.MurmurHash3
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin
import java.nio.ByteBuffer
import java.util.Base64

@Suppress("UtilityClassWithPublicConstructor")
class IndexUtils {
    companion object {
        @Suppress("ObjectPropertyNaming")
        const val _META = "_meta"
        const val PROPERTIES = "properties"
        const val FIELDS = "fields"
        const val SCHEMA_VERSION = "schema_version"
        const val DEFAULT_SCHEMA_VERSION = 1L
        const val ODFE_MAGIC_NULL = "#ODFE-MAGIC-NULL-MAGIC-ODFE#"
        private const val BYTE_ARRAY_SIZE = 16
        private const val DOCUMENT_ID_SEED = 72390L

        val logger = LogManager.getLogger(IndexUtils::class.java)

        var indexManagementConfigSchemaVersion: Long
            private set
        var indexStateManagementHistorySchemaVersion: Long
            private set

        init {
            indexManagementConfigSchemaVersion = getSchemaVersion(IndexManagementIndices.indexManagementMappings)
            indexStateManagementHistorySchemaVersion = getSchemaVersion(IndexManagementIndices.indexStateManagementHistoryMappings)
        }

        @Suppress("NestedBlockDepth")
        fun getSchemaVersion(mapping: String): Long {
            val xcp = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, mapping
            )

            while (!xcp.isClosed) {
                val token = xcp.currentToken()
                if (token != null && token != Token.END_OBJECT && token != Token.START_OBJECT) {
                    if (xcp.currentName() != _META) {
                        xcp.nextToken()
                        xcp.skipChildren()
                    } else {
                        while (xcp.nextToken() != Token.END_OBJECT) {
                            when (xcp.currentName()) {
                                SCHEMA_VERSION -> {
                                    val version = xcp.longValue()
                                    require(version > -1)
                                    return version
                                }
                                else -> xcp.nextToken()
                            }
                        }
                    }
                }
                xcp.nextToken()
            }
            return DEFAULT_SCHEMA_VERSION
        }

        fun shouldUpdateIndex(index: IndexMetadata, newVersion: Long): Boolean {
            var oldVersion = DEFAULT_SCHEMA_VERSION

            val indexMapping = index.mapping()?.sourceAsMap()
            if (indexMapping != null && indexMapping.containsKey(_META) && indexMapping[_META] is HashMap<*, *>) {
                val metaData = indexMapping[_META] as HashMap<*, *>
                if (metaData.containsKey(SCHEMA_VERSION)) {
                    oldVersion = (metaData[SCHEMA_VERSION] as Int).toLong()
                }
            }
            return newVersion > oldVersion
        }

        fun checkAndUpdateConfigIndexMapping(
            clusterState: ClusterState,
            client: IndicesAdminClient,
            actionListener: ActionListener<AcknowledgedResponse>
        ) {
            checkAndUpdateIndexMapping(
                IndexManagementPlugin.INDEX_MANAGEMENT_INDEX,
                indexManagementConfigSchemaVersion,
                IndexManagementIndices.indexManagementMappings,
                clusterState,
                client,
                actionListener
            )
        }

        fun checkAndUpdateHistoryIndexMapping(
            clusterState: ClusterState,
            client: IndicesAdminClient,
            actionListener: ActionListener<AcknowledgedResponse>
        ) {
            checkAndUpdateAliasMapping(
                IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS,
                indexStateManagementHistorySchemaVersion,
                IndexManagementIndices.indexStateManagementHistoryMappings,
                clusterState,
                client,
                actionListener
            )
        }

        @OpenForTesting
        @Suppress("LongParameterList")
        fun checkAndUpdateIndexMapping(
            index: String,
            schemaVersion: Long,
            mapping: String,
            clusterState: ClusterState,
            client: IndicesAdminClient,
            actionListener: ActionListener<AcknowledgedResponse>
        ) {
            if (clusterState.metadata.indices.containsKey(index)) {
                if (shouldUpdateIndex(clusterState.metadata.indices[index], schemaVersion)) {
                    val putMappingRequest: PutMappingRequest = PutMappingRequest(index).source(mapping, XContentType.JSON)
                    client.putMapping(putMappingRequest, actionListener)
                } else {
                    actionListener.onResponse(AcknowledgedResponse(true))
                }
            } else {
                logger.error("IndexMetaData does not exist for $index")
                actionListener.onResponse(AcknowledgedResponse(false))
            }
        }

        @OpenForTesting
        @Suppress("LongParameterList")
        fun checkAndUpdateAliasMapping(
            alias: String,
            schemaVersion: Long,
            mapping: String,
            clusterState: ClusterState,
            client: IndicesAdminClient,
            actionListener: ActionListener<AcknowledgedResponse>
        ) {
            val result = clusterState.metadata.indicesLookup[alias]
            if (result == null || result.type != IndexAbstraction.Type.ALIAS) {
                logger.error("There are no indices for alias $alias")
                actionListener.onResponse(AcknowledgedResponse(false))
            } else {
                val writeIndex = result.writeIndex
                if (writeIndex == null) {
                    logger.error("Concrete write index does not exist for alias $alias")
                    actionListener.onResponse(AcknowledgedResponse(false))
                } else {
                    if (shouldUpdateIndex(writeIndex, schemaVersion)) {
                        val putMappingRequest: PutMappingRequest = PutMappingRequest(writeIndex.index.name)
                            .source(mapping, XContentType.JSON)
                        client.putMapping(putMappingRequest, actionListener)
                    } else {
                        actionListener.onResponse(AcknowledgedResponse(true))
                    }
                }
            }
        }

        fun getFieldFromMappings(fieldName: String, mappings: Map<*, *>): Map<*, *>? {
            var currMap = mappings
            fieldName.split(".").forEach { field ->
                val nextMap = (currMap[PROPERTIES] as Map<*, *>? ?: currMap[FIELDS] as Map<*, *>?)?.get(field) ?: return null
                currMap = nextMap as Map<*, *>
            }

            return currMap
        }

        fun hashToFixedSize(id: String): String {
            val docByteArray = id.toByteArray()
            val hash = MurmurHash3.hash128(docByteArray, 0, docByteArray.size, DOCUMENT_ID_SEED, MurmurHash3.Hash128())
            val byteArray = ByteBuffer.allocate(BYTE_ARRAY_SIZE).putLong(hash.h1).putLong(hash.h2).array()
            return Base64.getUrlEncoder().withoutPadding().encodeToString(byteArray)
        }
    }
}
