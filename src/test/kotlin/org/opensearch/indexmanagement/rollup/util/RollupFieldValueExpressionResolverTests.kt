/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.util

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import com.nhaarman.mockitokotlin2.doReturn
import org.junit.Before
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.ingest.TestTemplateService
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.test.OpenSearchTestCase

class RollupFieldValueExpressionResolverTests : OpenSearchTestCase() {

    private val scriptService: ScriptService = mock()
    private val clusterService: ClusterService = mock()
    private val indexAliasUtils: RollupFieldValueExpressionResolver.IndexAliasUtils = mock()
    @Before
    fun settings() {
        RollupFieldValueExpressionResolver.registerServices(scriptService, clusterService)
        clusterService.state()
    }

    fun `test resolving no alias successfully`() {
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory("test_index_123"))
        whenever(indexAliasUtils.hasAlias(any())).doReturn(false)
        val rollup = randomRollup().copy(sourceIndex = "test_index_123", targetIndex = "{{ctx.source_index}}")
        val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(rollup, rollup.targetIndex)
        assertEquals("test_index_123", targetIndexResolvedName)
    }

    fun `test resolving with alias successfully`() {
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory("test_index_123"))
        whenever(indexAliasUtils.hasAlias(any())).doReturn(true)
        whenever(indexAliasUtils.getWriteIndexNameForAlias(any())).doReturn("backing_index")
        val rollup = randomRollup().copy(sourceIndex = "test_index_123", targetIndex = "{{ctx.source_index}}")
        val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(rollup, rollup.targetIndex)
        assertEquals("backing_index", targetIndexResolvedName)
    }

    fun `test resolving failed returned passed value`() {
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory(""))
        val rollup = randomRollup().copy(sourceIndex = "test_index_123", targetIndex = "{{ctx.source_index}}")
        val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(rollup, rollup.targetIndex)
        assertEquals("{{ctx.source_index}}", targetIndexResolvedName)
    }
}
