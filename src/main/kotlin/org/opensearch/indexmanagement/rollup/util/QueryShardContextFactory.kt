/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.util

import org.opensearch.Version
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.NamedWriteableRegistry
import org.opensearch.common.regex.Regex
import org.opensearch.common.settings.IndexScopedSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.settings.SettingsModule
import org.opensearch.common.util.BigArrays
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.env.Environment
import org.opensearch.env.NodeEnvironment
import org.opensearch.index.Index
import org.opensearch.index.IndexSettings
import org.opensearch.index.mapper.MapperService
import org.opensearch.index.query.QueryShardContext
import org.opensearch.index.similarity.SimilarityService
import org.opensearch.indices.IndicesModule
import org.opensearch.indices.analysis.AnalysisModule
import org.opensearch.plugins.MapperPlugin
import org.opensearch.plugins.PluginsService
import org.opensearch.script.ScriptService
import java.time.Instant

object QueryShardContextFactory {
    lateinit var client: Client
    lateinit var clusterService: ClusterService
    lateinit var scriptService: ScriptService
    lateinit var xContentRegistry: NamedXContentRegistry
    lateinit var environment: Environment
    lateinit var nodeEnvironment: NodeEnvironment
    lateinit var namedWriteableRegistry: NamedWriteableRegistry
    lateinit var indexNameExpressionResolver: IndexNameExpressionResolver

    fun init(
        client: Client,
        clusterService: ClusterService,
        scriptService: ScriptService,
        xContentRegistry: NamedXContentRegistry,
        environment: Environment,
        nodeEnvironment: NodeEnvironment,
        namedWriteableRegistry: NamedWriteableRegistry,
        indexNameExpressionResolver: IndexNameExpressionResolver
    ) {
        this.client = client
        this.clusterService = clusterService
        this.scriptService = scriptService
        this.xContentRegistry = xContentRegistry
        this.environment = environment
        this.nodeEnvironment = nodeEnvironment
        this.namedWriteableRegistry = namedWriteableRegistry
        this.indexNameExpressionResolver = indexNameExpressionResolver
    }

    fun createShardContext(indexName: String?): QueryShardContext {

        var index: Index?
        var indexSettings: Settings?
        val indexMetadata = clusterService.state().metadata.index(indexName)
        if (indexMetadata != null) {
            index = indexMetadata.index
            indexSettings = indexMetadata.settings
        } else {
            index = Index("dummyIndexName", "randomindexuuid123456")
            indexSettings = Settings.EMPTY
        }
        val nodeSettings = Settings.builder()
            .put("node.name", "dummyNodeName")
            .put(Environment.PATH_HOME_SETTING.key, environment.tmpFile())
            .build()
        val pluginsService =
            PluginsService(nodeSettings, null, environment.modulesFile(), environment.pluginsFile(), listOf())
        val additionalSettings = pluginsService.pluginSettings
        val settingsModule = SettingsModule(
            nodeSettings,
            additionalSettings,
            pluginsService.pluginSettingsFilter, emptySet()
        )
        val indexScopedSettings: IndexScopedSettings = settingsModule.getIndexScopedSettings()
        val idxSettings = newIndexSettings(index, indexSettings, indexScopedSettings)
        val indicesModule = IndicesModule(pluginsService.filterPlugins(MapperPlugin::class.java))
        val mapperRegistry = indicesModule.mapperRegistry
        val analysisModule = AnalysisModule(environment, emptyList())
        val indexAnalyzers = analysisModule.analysisRegistry.build(idxSettings)
        val similarityService = SimilarityService(idxSettings, null, emptyMap())
        val mapperService = MapperService(
            idxSettings,
            indexAnalyzers,
            xContentRegistry,
            similarityService,
            mapperRegistry,
            { createShardContext(null) },
            { false },
            scriptService
        )

        mapperService.merge("_doc", indexMetadata.mapping()?.source(), MapperService.MergeReason.MAPPING_UPDATE)

        return QueryShardContext(
            0,
            idxSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            mapperService,
            null,
            scriptService,
            xContentRegistry,
            namedWriteableRegistry,
            null,
            null,
            { Instant.now().toEpochMilli() },
            null,
            { pattern -> Regex.simpleMatch(pattern, index?.name) },
            { true },
            null
        )
    }

    fun newIndexSettings(index: Index?, settings: Settings?, indexScopedSettings: IndexScopedSettings?): IndexSettings? {
        val build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(settings)
            .build()
        val metadata = IndexMetadata.builder(index?.name).settings(build).build()
        return IndexSettings(metadata, Settings.EMPTY, indexScopedSettings)
    }
}
