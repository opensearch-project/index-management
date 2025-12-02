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
    lateinit var indexAliasUtils: IndexAliasUtils

    /**
     * Resolves template variables in a field value using the rollup object as context.
     * This method is kept for backward compatibility and only supports {{ctx.source_index}}.
     *
     * @param rollup The rollup object providing context for template resolution
     * @param fieldValue The field value that may contain template variables (e.g., "{{ctx.source_index}}")
     * @return The resolved field value with variables replaced, or the original value if resolution
     *         produces an empty/null result
     */
    fun resolve(rollup: Rollup, fieldValue: String): String {
        val script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, fieldValue, mapOf())

        val contextMap =
            rollup.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)
                .toMap()
                .filterKeys { key -> key in validTopContextFields }

        var compiledValue =
            scriptService.compile(script, TemplateScript.CONTEXT)
                .newInstance(script.params + mapOf("ctx" to contextMap))
                .execute()

        if (indexAliasUtils.isAlias(compiledValue)) {
            compiledValue = indexAliasUtils.getWriteIndexNameForAlias(compiledValue)
        }

        return if (compiledValue.isNullOrBlank()) fieldValue else compiledValue
    }

    /**
     * Resolves template variables in a field value using the rollup object and managed index name as context.
     * This is the primary method used by ISM rollup actions to resolve source_index and target_index templates.
     *
     * This method extends the basic resolve() by adding the managed index name to the template context,
     * enabling the use of {{ctx.index}} in addition to {{ctx.source_index}}.
     *
     * @param rollup The rollup object providing context for template resolution. The rollup's source_index
     *               field is made available as {{ctx.source_index}} in templates.
     * @param fieldValue The field value that may contain Mustache template variables. Common patterns:
     *                   - "{{ctx.index}}" - resolves to the managed index name
     *                   - "{{ctx.source_index}}" - resolves to the rollup's source index
     *                   - "rollup_{{ctx.index}}" - adds a prefix to the managed index name
     *                   - Literal values without templates are returned unchanged
     * @param managedIndexName The name of the index being managed by ISM. This is the index to which
     *                         the ISM policy is applied, and it's made available as {{ctx.index}} in templates.
     *                         For multi-tier rollups, this would be the output of the previous tier.
     * @return The resolved field value with all template variables replaced. If the resolved value is an alias,
     *         it's automatically resolved to the write index name. If resolution produces an empty or null result,
     *         the original fieldValue is returned unchanged.
     */
    fun resolve(rollup: Rollup, fieldValue: String, managedIndexName: String): String {
        val script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, fieldValue, mapOf())

        val contextMap =
            rollup.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)
                .toMap()
                .filterKeys { key -> key in validTopContextFields }
                .plus("index" to managedIndexName)

        var compiledValue =
            scriptService.compile(script, TemplateScript.CONTEXT)
                .newInstance(script.params + mapOf("ctx" to contextMap))
                .execute()

        if (indexAliasUtils.isAlias(compiledValue)) {
            compiledValue = indexAliasUtils.getWriteIndexNameForAlias(compiledValue)
        }

        return if (compiledValue.isNullOrBlank()) fieldValue else compiledValue
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

    open class IndexAliasUtils(val clusterService: ClusterService) {
        open fun hasAlias(index: String): Boolean {
            val aliases = this.clusterService.state().metadata().indices[index]?.aliases
            if (aliases != null) {
                return aliases.isNotEmpty()
            }
            return false
        }

        open fun isAlias(index: String): Boolean = this.clusterService.state().metadata().indicesLookup?.get(index) is IndexAbstraction.Alias

        open fun getWriteIndexNameForAlias(alias: String): String? = this.clusterService.state().metadata().indicesLookup?.get(alias)?.writeIndex?.index?.name

        open fun getBackingIndicesForAlias(alias: String): MutableList<IndexMetadata>? =
            this.clusterService.state().metadata().indicesLookup?.get(alias)?.indices
    }
}
