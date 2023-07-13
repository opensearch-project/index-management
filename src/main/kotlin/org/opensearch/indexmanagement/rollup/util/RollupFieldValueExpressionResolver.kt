/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.util

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.metadata.IndexAbstraction
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import org.opensearch.indexmanagement.opensearchapi.toMap
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript

object RollupFieldValueExpressionResolver {

    private val validTopContextFields = setOf(Rollup.SOURCE_INDEX_FIELD)

    private lateinit var scriptService: ScriptService
    private lateinit var clusterService: ClusterService
    lateinit var indexAliasUtils: IndexAliasUtils

    private val log = LogManager.getLogger(javaClass)

    /**
     * This method is currently used to resolve the target_index
     * target_index support dynamic scripting and alias type
     */
    fun resolve(rollup: Rollup, fieldValue: String): String {
        val script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, fieldValue, mapOf())

        val contextMap = rollup.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)
            .toMap()
            .filterKeys { key -> key in validTopContextFields }

        var compiledValue = scriptService.compile(script, TemplateScript.CONTEXT)
            .newInstance(script.params + mapOf("ctx" to contextMap))
            .execute()

        if (indexAliasUtils.isAlias(compiledValue)) {
            compiledValue = indexAliasUtils.getWriteIndexNameForAlias(compiledValue)
        }

        val res = if (compiledValue.isNullOrBlank()) fieldValue else compiledValue
        log.debug("Rollup field [$fieldValue] is resolved to $res")
        return res
    }

    fun registerServices(scriptService: ScriptService, clusterService: ClusterService) {
        this.scriptService = scriptService
        this.clusterService = clusterService
        this.indexAliasUtils = IndexAliasUtils(clusterService)
    }

    fun registerServices(scriptService: ScriptService, clusterService: ClusterService, indexAliasUtils: IndexAliasUtils) {
        this.scriptService = scriptService
        this.clusterService = clusterService
        this.indexAliasUtils = indexAliasUtils
    }

    @OpenForTesting
    class IndexAliasUtils(val clusterService: ClusterService) {
        fun hasAlias(index: String): Boolean {
            val aliases = this.clusterService.state().metadata().indices[index]?.aliases
            if (aliases != null) {
                return aliases.isNotEmpty()
            }
            return false
        }

        fun isAlias(index: String): Boolean {
            return this.clusterService.state().metadata().indicesLookup?.get(index) is IndexAbstraction.Alias
        }

        fun getWriteIndexNameForAlias(alias: String): String? {
            return this.clusterService.state().metadata().indicesLookup?.get(alias)?.writeIndex?.index?.name
        }

        fun getBackingIndicesForAlias(alias: String): MutableList<IndexMetadata>? {
            return this.clusterService.state().metadata().indicesLookup?.get(alias)?.indices
        }
    }
}
