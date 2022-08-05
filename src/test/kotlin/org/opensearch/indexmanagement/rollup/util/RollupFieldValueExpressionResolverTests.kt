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
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.ingest.TestTemplateService
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.test.OpenSearchTestCase

class RollupFieldValueExpressionResolverTests : OpenSearchTestCase() {

    private val scriptService: ScriptService = mock()

    @Before
    fun settings() {
        RollupFieldValueExpressionResolver.registerScriptService(scriptService)
    }

    fun `test resolving successfully`() {
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory("test_index_123"))
        val rollup = randomRollup().copy(sourceIndex = "test_index_123", targetIndex = "{{ctx.source_index}}")
        val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(rollup, rollup.targetIndex)
        assertEquals("test_index_123", targetIndexResolvedName)
    }

    fun `test resolving failed returned passed value`() {
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(TestTemplateService.MockTemplateScript.Factory(""))
        val rollup = randomRollup().copy(sourceIndex = "test_index_123", targetIndex = "{{ctx.source_index}}")
        val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(rollup, rollup.targetIndex)
        assertEquals("{{ctx.source_index}}", targetIndexResolvedName)
    }
}
