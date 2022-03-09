/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ISMIndexMetadata

/**
 * ISM by default considers all the index metadata to be part of the cluster state,
 * if that doesn't hold true and indices metadata is present in some other place and
 * ISM still need to manage these indices the following interface provides a mechanism
 * for ISM extensions to register the metadata service for the type so ISM can get the
 * index metadata for these special type of indices.
 *
 * ISM Rest APIs allows support for type param which determines the type of index, if there
 * is a registered metadata service for the type - ISM will use the service to get the metadata
 * else uses the default i.e cluster state
 */
interface IndexMetadataService {

    /**
     * Returns the index metadata needed for ISM
     */
    suspend fun getMetadata(indices: List<String>, client: Client, clusterService: ClusterService): Map<String, ISMIndexMetadata>

    /**
     * Returns all the indices metadata
     */
    suspend fun getMetadataForAllIndices(client: Client, clusterService: ClusterService): Map<String, ISMIndexMetadata>

    /**
     * Returns an optional setting path which, when set to true in the index settings, overrides a cluster level metadata write block.
     */
    fun getIndexMetadataWriteOverrideSetting(): String? = null
}
