/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.util

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.junit.Before
import org.mockito.ArgumentMatchers.anyString
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.ingest.TestTemplateService
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.test.OpenSearchTestCase

class RollupFieldValueExpressionResolverTests : OpenSearchTestCase() {
    private val scriptService: ScriptService = mock()
    private val clusterService: ClusterService = mock()
    private var indexAliasUtils: RollupFieldValueExpressionResolver.IndexAliasUtils = mock()

    @Before
    fun settings() {
        this.indexAliasUtils = mock()
        RollupFieldValueExpressionResolver.registerServices(scriptService, clusterService, indexAliasUtils)
    }

    fun `test resolving no alias successfully`() {
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory("test_index_123"))
        whenever(indexAliasUtils.isAlias(anyString())).doReturn(false)
        val rollup = randomRollup().copy(sourceIndex = "test_index_123", targetIndex = "{{ctx.source_index}}")
        val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(rollup, rollup.targetIndex)
        assertEquals("test_index_123", targetIndexResolvedName)
    }

    fun `test resolving with alias successfully`() {
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory("test_index_123"))
        whenever(indexAliasUtils.isAlias(anyString())).doReturn(true)
        whenever(indexAliasUtils.getWriteIndexNameForAlias(anyString())).doReturn("backing_index")
        val rollup = randomRollup().copy(sourceIndex = "test_index_123", targetIndex = "{{ctx.source_index}}")
        val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(rollup, rollup.targetIndex)
        assertEquals("backing_index", targetIndexResolvedName)
    }

    fun `test resolving failed returned passed value`() {
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory(""))
        whenever(indexAliasUtils.isAlias(anyString())).doReturn(false)
        val rollup = randomRollup().copy(sourceIndex = "test_index_123", targetIndex = "{{ctx.source_index}}")
        val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(rollup, rollup.targetIndex)
        assertEquals("{{ctx.source_index}}", targetIndexResolvedName)
    }

    fun `test resolving ctx index in source_index template`() {
        val managedIndexName = "logs-2024-01"
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory(managedIndexName))
        whenever(indexAliasUtils.isAlias(anyString())).doReturn(false)
        val rollup = randomRollup().copy(sourceIndex = "test_index_123")
        val resolvedSourceIndex = RollupFieldValueExpressionResolver.resolve(rollup, "{{ctx.index}}", managedIndexName)
        assertEquals(managedIndexName, resolvedSourceIndex)
    }

    fun `test resolving ctx index in target_index template`() {
        val managedIndexName = "logs-2024-01"
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory("rollup_$managedIndexName"))
        whenever(indexAliasUtils.isAlias(anyString())).doReturn(false)
        val rollup = randomRollup().copy(sourceIndex = "test_index_123")
        val resolvedTargetIndex = RollupFieldValueExpressionResolver.resolve(rollup, "rollup_{{ctx.index}}", managedIndexName)
        assertEquals("rollup_$managedIndexName", resolvedTargetIndex)
    }

    fun `test resolving ctx index with different managed index names`() {
        val managedIndexNames = listOf("index-1", "index-2", "data-stream-000001")
        managedIndexNames.forEach { managedIndexName ->
            whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory(managedIndexName))
            whenever(indexAliasUtils.isAlias(anyString())).doReturn(false)
            val rollup = randomRollup().copy(sourceIndex = "test_index")
            val resolvedIndex = RollupFieldValueExpressionResolver.resolve(rollup, "{{ctx.index}}", managedIndexName)
            assertEquals(managedIndexName, resolvedIndex)
        }
    }

    fun `test resolving ctx source_index in source_index template`() {
        val sourceIndexName = "raw-data-index"
        val managedIndexName = "managed-index"
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory(sourceIndexName))
        whenever(indexAliasUtils.isAlias(anyString())).doReturn(false)
        val rollup = randomRollup().copy(sourceIndex = sourceIndexName)
        val resolvedSourceIndex = RollupFieldValueExpressionResolver.resolve(rollup, "{{ctx.source_index}}", managedIndexName)
        assertEquals(sourceIndexName, resolvedSourceIndex)
    }

    fun `test resolving ctx source_index in target_index template`() {
        val sourceIndexName = "raw-data-index"
        val managedIndexName = "managed-index"
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory("rollup_$sourceIndexName"))
        whenever(indexAliasUtils.isAlias(anyString())).doReturn(false)
        val rollup = randomRollup().copy(sourceIndex = sourceIndexName)
        val resolvedTargetIndex = RollupFieldValueExpressionResolver.resolve(rollup, "rollup_{{ctx.source_index}}", managedIndexName)
        assertEquals("rollup_$sourceIndexName", resolvedTargetIndex)
    }

    fun `test resolving template with both ctx index and ctx source_index`() {
        val sourceIndexName = "raw-data"
        val managedIndexName = "logs-2024-01"
        val expectedResolved = "rollup_${managedIndexName}_from_$sourceIndexName"
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory(expectedResolved))
        whenever(indexAliasUtils.isAlias(anyString())).doReturn(false)
        val rollup = randomRollup().copy(sourceIndex = sourceIndexName)
        val resolvedIndex = RollupFieldValueExpressionResolver.resolve(
            rollup,
            "rollup_{{ctx.index}}_from_{{ctx.source_index}}",
            managedIndexName,
        )
        assertEquals(expectedResolved, resolvedIndex)
    }

    fun `test resolving template with ctx index for multi-tier rollup`() {
        // Simulates second tier rollup where managed index is the first tier rollup output
        val managedIndexName = "rollup_tier1_logs-2024-01"
        val expectedResolved = "rollup_tier2_$managedIndexName"
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory(expectedResolved))
        whenever(indexAliasUtils.isAlias(anyString())).doReturn(false)
        val rollup = randomRollup().copy(sourceIndex = managedIndexName)
        val resolvedTargetIndex = RollupFieldValueExpressionResolver.resolve(
            rollup,
            "rollup_tier2_{{ctx.index}}",
            managedIndexName,
        )
        assertEquals(expectedResolved, resolvedTargetIndex)
    }

    fun `test alias resolution with templated source_index`() {
        // Test where the resolved value from template is an alias
        val managedIndexName = "logs-alias"
        val backingIndexName = "logs-2024-01-backing"

        // Mock the template to resolve to the alias name
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory(managedIndexName))

        // Mock that the resolved value is an alias
        whenever(indexAliasUtils.isAlias(eq(managedIndexName))).doReturn(true)

        // Mock the write index resolution
        whenever(indexAliasUtils.getWriteIndexNameForAlias(eq(managedIndexName))).doReturn(backingIndexName)

        val rollup = randomRollup().copy(sourceIndex = "test_index")
        val resolvedSourceIndex = RollupFieldValueExpressionResolver.resolve(rollup, "{{ctx.index}}", managedIndexName)

        // Verify it resolves to the backing write index, not the alias
        assertEquals(backingIndexName, resolvedSourceIndex)
    }
}
