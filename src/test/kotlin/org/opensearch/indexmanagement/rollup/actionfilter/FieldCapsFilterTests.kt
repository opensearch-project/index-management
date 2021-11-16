/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.actionfilter

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.junit.Before
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.randomISMFieldCapabilitiesIndexResponse
import org.opensearch.indexmanagement.rollup.randomISMFieldCaps
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.indexmanagement.rollup.settings.RollupSettings
import org.opensearch.test.OpenSearchTestCase

class FieldCapsFilterTests : OpenSearchTestCase() {
    private val indexNameExpressionResolver: IndexNameExpressionResolver = mock()
    private val clusterService: ClusterService = mock()
    private val clusterState: ClusterState = mock()
    private val metadata: Metadata = mock()
    private val settings: Settings = Settings.EMPTY
    private val indexMetadata: IndexMetadata = mock()
    private val rollupIndex: String = "dummy-rollupIndex"
    private val rollup: Rollup = randomRollup()

    @Before
    fun setupSettings() {
        whenever(clusterService.clusterSettings).doReturn(ClusterSettings(Settings.EMPTY, setOf(RollupSettings.ROLLUP_DASHBOARDS)))
        whenever(clusterService.state()).doReturn(clusterState)
        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollupIndex)).doReturn(indexMetadata)
    }

    fun `test rewrite unmerged response`() {
        val fieldCapFilter = FieldCapsFilter(clusterService, settings, indexNameExpressionResolver)
        val originalIsmResponse = ISMFieldCapabilitiesResponse(arrayOf(), mapOf(), listOf(randomISMFieldCapabilitiesIndexResponse()))
        val rewrittenResponse = fieldCapFilter.rewriteResponse(originalIsmResponse.toFieldCapabilitiesResponse(), setOf(rollupIndex), false) as FieldCapabilitiesResponse
        val rewrittenIsmResponse = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(rewrittenResponse)
        assertEquals("Expected merged response to be empty, indices not empty", 0, rewrittenResponse.indices.size)
        assertEquals("Expected merged response to be empty, map is empty", 0, rewrittenResponse.get().size)
        assertEquals("Expected unmerged response sizes are different", originalIsmResponse.indexResponses.size + 1, rewrittenIsmResponse.indexResponses.size)
    }

    fun `test rewrite unmerged response discarding existing response`() {
        val fieldCapFilter = FieldCapsFilter(clusterService, settings, indexNameExpressionResolver)
        val originalIsmResponse = ISMFieldCapabilitiesResponse(arrayOf(), mapOf(), listOf(randomISMFieldCapabilitiesIndexResponse()))
        val rewrittenResponse = fieldCapFilter.rewriteResponse(originalIsmResponse.toFieldCapabilitiesResponse(), setOf(rollupIndex), true) as
            FieldCapabilitiesResponse
        val rewrittenIsmResponse = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(rewrittenResponse)
        assertEquals("Expected merged response to be empty, indices not empty", 0, rewrittenResponse.indices.size)
        assertEquals("Expected merged response to be empty, map is empty", 0, rewrittenResponse.get().size)
        assertEquals("Expected unmerged response sizes are different", 1, rewrittenIsmResponse.indexResponses.size)
    }

    fun `test rewrite merged response`() {
        val fieldCapFilter = FieldCapsFilter(clusterService, settings, indexNameExpressionResolver)
        val ismResponse = randomISMFieldCaps()
        val originalIsmResponse = ISMFieldCapabilitiesResponse(ismResponse.indices, ismResponse.responseMap, listOf())
        val rewrittenResponse = fieldCapFilter.rewriteResponse(originalIsmResponse.toFieldCapabilitiesResponse(), setOf(rollupIndex), true) as
            FieldCapabilitiesResponse
        val rewrittenIsmResponse = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(rewrittenResponse)
        assertTrue("Expected unmerged response to be empty", rewrittenIsmResponse.indexResponses.isEmpty())
    }
}
