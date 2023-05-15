/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexRequestBuilder
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.action.admin.indices.refresh.RefreshAction
import org.opensearch.action.admin.indices.refresh.RefreshRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.plugins.Plugin
import org.opensearch.test.OpenSearchSingleNodeTestCase
import java.util.Locale

/**
 * A test that keep a singleton node started for all tests that can be used to get
 * references to Guice injectors in unit tests.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
abstract class ISMSingleNodeTestCase : OpenSearchSingleNodeTestCase() {

    protected val index: String = randomAlphaOfLength(10).lowercase(Locale.ROOT)

    override fun setUp() {
        super.setUp()
        createTestIndex()
    }

    protected fun getAllIndicesFromPattern(pattern: String): List<String> {
        val getIndexResponse = (
            client().admin().indices().prepareGetIndex()
                .setIndices(pattern) as GetIndexRequestBuilder
            ).get() as GetIndexResponse
        getIndexResponse
        return getIndexResponse.indices().toList()
    }

    /** A test index that can be used across tests. Feel free to add new fields but don't remove any. */
    protected fun createTestIndex() {
        createIndex(
            index, Settings.EMPTY,
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" }
                }
            """.trimIndent()
        )
    }

    protected fun indexDoc(index: String, id: String, doc: String) {
        client().prepareIndex(index).setId(id)
            .setSource(doc, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get()
    }

    protected fun assertIndexExists(index: String) {
        val getIndexResponse =
            client().admin().indices().getIndex(
                GetIndexRequest().indices(index).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
            ).get()
        assertTrue(getIndexResponse.indices.size > 0)
    }

    protected fun assertIndexNotExists(index: String) {
        val getIndexResponse =
            client().admin().indices().getIndex(
                GetIndexRequest().indices(index).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
            ).get()
        assertFalse(getIndexResponse.indices.size > 0)
    }

    protected fun assertAliasNotExists(alias: String) {
        val aliasesResponse = client().admin().indices().getAliases(GetAliasesRequest()).get()
        val foundAlias = aliasesResponse.aliases.values.forEach {
            it.forEach {
                if (it.alias == alias) {
                    fail("alias exists, but it shouldn't")
                }
            }
        }
    }

    protected fun assertAliasExists(alias: String) {
        val aliasesResponse = client().admin().indices().getAliases(GetAliasesRequest()).get()
        val foundAlias = aliasesResponse.aliases.values.forEach {
            it.forEach {
                if (it.alias == alias) {
                    return
                }
            }
        }
        fail("alias doesn't exists, but it should")
    }

    protected fun refreshIndex(index: String) {
        client().execute(RefreshAction.INSTANCE, RefreshRequest(index)).get()
    }

    override fun getPlugins(): List<Class<out Plugin>> {
        return listOf(IndexManagementPlugin::class.java)
    }

    override fun resetNodeAfterTest(): Boolean {
        return false
    }
}
