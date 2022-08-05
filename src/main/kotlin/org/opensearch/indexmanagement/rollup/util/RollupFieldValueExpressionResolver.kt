/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.util

import org.opensearch.cluster.metadata.IndexAbstraction
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import org.opensearch.indexmanagement.opensearchapi.toMap
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript

object RollupFieldValueExpressionResolver {

    private val validTopContextFields = setOf(Rollup.SOURCE_INDEX_FIELD)

    private lateinit var scriptService: ScriptService
    private lateinit var clusterService: ClusterService
    fun resolve(rollup: Rollup, fieldValue: String): String {
        val script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, fieldValue, mapOf())

        val contextMap = rollup.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)
            .toMap()
            .filterKeys { key -> key in validTopContextFields }

        var compiledValue = scriptService.compile(script, TemplateScript.CONTEXT)
            .newInstance(script.params + mapOf("ctx" to contextMap))
            .execute()

        if (isAlias(compiledValue)) {
            compiledValue = getWriteIndexNameForAlias(compiledValue)
        }

        return if (compiledValue.isNullOrBlank()) fieldValue else compiledValue
    }

    fun registerScriptService(scriptService: ScriptService) {
        this.scriptService = scriptService
    }
    fun hasAlias(index: String): Boolean {
        val aliases = clusterService.state().metadata().indices.get(index)?.aliases
        if (aliases != null) {
            return aliases.size() > 0
        }
        return false
    }
    fun isAlias(index: String): Boolean {
        return clusterService.state().metadata().indicesLookup?.get(index) is IndexAbstraction.Alias
    }
    fun getWriteIndexNameForAlias(alias: String): String? {
        return clusterService.state().metadata().indicesLookup?.get(alias)?.writeIndex?.index?.name
    }

    fun getBackingIndicesForAlias(alias: String): MutableList<IndexMetadata>? {
        return clusterService.state().metadata().indicesLookup?.get(alias)?.indices
    }

    fun registerServices(scriptService: ScriptService, clusterService: ClusterService) {
        this.scriptService = scriptService
        this.clusterService = clusterService
    }
}
