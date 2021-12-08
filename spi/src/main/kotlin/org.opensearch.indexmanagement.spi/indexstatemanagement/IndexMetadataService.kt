/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ISMIndexMetadata

interface IndexMetadataService {

    /**
     * Returns the index metadata needed for ISM
     */
    fun getMetadata(indices: List<String>, client: Client, clusterService: ClusterService): Map<String, ISMIndexMetadata>
}
