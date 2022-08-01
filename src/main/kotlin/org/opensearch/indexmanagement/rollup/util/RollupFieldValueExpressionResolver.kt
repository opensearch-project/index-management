/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.util

import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript

object RollupFieldValueExpressionResolver {

    private val validTopContextFields = setOf(Rollup.SOURCE_INDEX_FIELD)

    private lateinit var scriptService: ScriptService

    fun resolve(rollup: Rollup, fieldValue: String): String {
        val script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, fieldValue, mapOf())

        val contextMap = XContentHelper.convertToMap(
            BytesReference.bytes(rollup.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)),
            false,
            XContentType.JSON
        ).v2().filterKeys { key ->
            key in validTopContextFields
        }

        val compiledValue = scriptService.compile(script, TemplateScript.CONTEXT)
            .newInstance(script.params + mapOf("ctx" to contextMap))
            .execute()

        return if (compiledValue.isBlank()) fieldValue else compiledValue
    }

    fun registerScriptService(scriptService: ScriptService) {
        this.scriptService = scriptService
    }
}
