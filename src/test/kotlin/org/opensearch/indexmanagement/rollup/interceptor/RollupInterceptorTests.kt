/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import org.junit.Before
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.Companion.BYPASS_ROLLUP_SEARCH
import org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.Companion.BYPASS_SIZE_CHECK
import org.opensearch.indexmanagement.rollup.settings.RollupSettings
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.search.internal.ShardSearchRequest
import org.opensearch.test.OpenSearchTestCase

class RollupInterceptorTests : OpenSearchTestCase() {

    private lateinit var interceptor: RollupInterceptor

    @Before
    fun setup() {
        interceptor = createInterceptor()
    }

    fun `test getBypassFromFetchSource returns null when no source`() {
        val request = mock(ShardSearchRequest::class.java)
        `when`(request.source()).thenReturn(null)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertNull(bypassLevel)
    }

    fun `test getBypassFromFetchSource returns null when no FetchSourceContext`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertNull(bypassLevel)
    }

    fun `test getBypassFromFetchSource returns null when no includes array`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(FetchSourceContext(true))

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertNull(bypassLevel)
    }

    fun `test getBypassFromFetchSource returns null when no bypass marker present`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(FetchSourceContext(false, arrayOf("field1", "field2"), emptyArray()))

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertNull(bypassLevel)
    }

    fun `test getBypassFromFetchSource extracts BYPASS_ROLLUP_SEARCH correctly`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(FetchSourceContext(false, arrayOf("_rollup_internal_bypass_$BYPASS_ROLLUP_SEARCH"), emptyArray()))

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertEquals(BYPASS_ROLLUP_SEARCH, bypassLevel)
    }

    fun `test getBypassFromFetchSource extracts BYPASS_SIZE_CHECK correctly`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(FetchSourceContext(false, arrayOf("_rollup_internal_bypass_$BYPASS_SIZE_CHECK"), emptyArray()))

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertEquals(BYPASS_SIZE_CHECK, bypassLevel)
    }

    fun `test getBypassFromFetchSource finds marker among multiple includes`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(
            FetchSourceContext(false, arrayOf("field1", "_rollup_internal_bypass_$BYPASS_ROLLUP_SEARCH", "field2"), emptyArray()),
        )

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertEquals(BYPASS_ROLLUP_SEARCH, bypassLevel)
    }

    fun `test getBypassFromFetchSource returns null for invalid bypass marker`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(FetchSourceContext(false, arrayOf("_rollup_internal_bypass_invalid"), emptyArray()))

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertNull(bypassLevel)
    }

    // Helper method to create interceptor instance
    private fun createInterceptor(): RollupInterceptor {
        val clusterService = mock(ClusterService::class.java)
        val clusterSettings = ClusterSettings(
            Settings.EMPTY,
            setOf(
                RollupSettings.ROLLUP_SEARCH_ENABLED,
                RollupSettings.ROLLUP_SEARCH_ALL_JOBS,
                RollupSettings.ROLLUP_SEARCH_SOURCE_INDICES,
            ),
        )
        `when`(clusterService.clusterSettings).thenReturn(clusterSettings)

        val settings = Settings.EMPTY
        val resolver = mock(IndexNameExpressionResolver::class.java)
        return RollupInterceptor(clusterService, settings, resolver)
    }
}
