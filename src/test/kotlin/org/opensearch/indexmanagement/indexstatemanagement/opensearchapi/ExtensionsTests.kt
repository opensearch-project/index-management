/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.opensearchapi

import org.opensearch.Version
import org.opensearch.action.admin.indices.rollover.RolloverInfo
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.test.OpenSearchTestCase

class ExtensionsTests : OpenSearchTestCase() {

    fun `test getting oldest rollover time`() {
        val noRolloverMetadata = IndexMetadata
            .Builder("foo-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build()

        assertNull(noRolloverMetadata.getOldestRolloverTime())
        val oldest = RolloverInfo("bar-alias", emptyList(), 17L)

        val metadata = IndexMetadata
            .Builder(noRolloverMetadata)
            .putRolloverInfo(RolloverInfo("foo-alias", emptyList(), 42L))
            .putRolloverInfo(oldest)
            .putRolloverInfo(RolloverInfo("baz-alias", emptyList(), 134345L))
            .build()

        assertEquals("Did not get the oldest rollover time", oldest.time, metadata.getOldestRolloverTime()?.toEpochMilli())
    }
}
