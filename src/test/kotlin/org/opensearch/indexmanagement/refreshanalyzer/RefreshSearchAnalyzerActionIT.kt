/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.refreshanalyzer

import org.junit.Assume
import org.junit.Before
import org.opensearch.client.Request
import org.opensearch.common.io.Streams
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.IndexManagementRestTestCase
import org.opensearch.indexmanagement.refreshanalyzer.RestRefreshSearchAnalyzerAction.Companion.REFRESH_SEARCH_ANALYZER_BASE_URI
import java.io.InputStreamReader
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class RefreshSearchAnalyzerActionIT : IndexManagementRestTestCase() {

    @Before
    fun checkIfLocalCluster() {
        Assume.assumeTrue(isLocalTest)
    }

    fun `test index time analyzer`() {
        val buildDir = System.getProperty("buildDir")
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val indexName = "testindex"

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola")
        }

        val settings: Settings = Settings.builder()
            .loadFromSource(getIndexAnalyzerSettings(), XContentType.JSON)
            .build()
        createIndex(indexName, settings, getAnalyzerMapping())
        ingestData(indexName)

        assertTrue(queryData(indexName, "hello").contains("hello world"))

        // check synonym
        val result2 = queryData(indexName, "hola")
        assertTrue(result2.contains("hello world"))

        // check non synonym
        val result3 = queryData(indexName, "namaste")
        assertFalse(result3.contains("hello world"))

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola, namaste")
        }

        // New added synonym should NOT match
        val result4 = queryData(indexName, "namaste")
        assertFalse(result4.contains("hello world"))

        // refresh synonyms
        refreshAnalyzer(indexName)

        // New added synonym should NOT match
        val result5 = queryData(indexName, "namaste")
        assertFalse(result5.contains("hello world"))

        // clean up
        for (i in 0 until numNodes) {
            deleteFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
        }
    }

    fun `test search time analyzer`() {
        val buildDir = System.getProperty("buildDir")
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val indexName = "testindex"

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola")
        }

        val settings: Settings = Settings.builder()
            .loadFromSource(getSearchAnalyzerSettings(), XContentType.JSON)
            .build()
        createIndex(indexName, settings, getAnalyzerMapping())
        ingestData(indexName)

        assertTrue(queryData(indexName, "hello").contains("hello world"))

        // check synonym
        val result2 = queryData(indexName, "hola")
        assertTrue(result2.contains("hello world"))

        // check non synonym
        val result3 = queryData(indexName, "namaste")
        assertFalse(result3.contains("hello world"))

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola, namaste")
        }

        // New added synonym should NOT match
        val result4 = queryData(indexName, "namaste")
        assertFalse(result4.contains("hello world"))

        // refresh synonyms
        refreshAnalyzer(indexName)

        // New added synonym should match
        val result5 = queryData(indexName, "namaste")
        assertTrue(result5.contains("hello world"))

        // clean up
        for (i in 0 until numNodes) {
            deleteFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
        }
    }

    fun `test alias`() {
        val indexName = "testindex"
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val buildDir = System.getProperty("buildDir")
        val aliasName = "test"
        val aliasSettings = "\"$aliasName\": {}"

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola")
        }

        val settings: Settings = Settings.builder()
            .loadFromSource(getSearchAnalyzerSettings(), XContentType.JSON)
            .build()
        createIndex(indexName, settings, getAnalyzerMapping(), aliasSettings)
        ingestData(indexName)

        assertTrue(queryData(indexName, "hello").contains("hello world"))

        // check synonym
        val result2 = queryData(aliasName, "hola")
        assertTrue(result2.contains("hello world"))

        // check non synonym
        val result3 = queryData(aliasName, "namaste")
        assertFalse(result3.contains("hello world"))

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola, namaste")
        }

        // New added synonym should NOT match
        val result4 = queryData(aliasName, "namaste")
        assertFalse(result4.contains("hello world"))

        // refresh synonyms
        refreshAnalyzer(aliasName)

        // New added synonym should match
        val result5 = queryData(aliasName, "namaste")
        assertTrue(result5.contains("hello world"))

        for (i in 0 until numNodes) {
            deleteFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
        }
    }

    companion object {

        fun writeToFile(filePath: String, contents: String) {
            val path = org.opensearch.common.io.PathUtils.get(filePath)
            Files.newBufferedWriter(path, Charset.forName("UTF-8")).use { writer -> writer.write(contents) }
        }

        fun deleteFile(filePath: String) {
            Files.deleteIfExists(org.opensearch.common.io.PathUtils.get(filePath))
        }

        fun ingestData(indexName: String) {
            val request = Request("POST", "/$indexName/_doc?refresh=true")
            val data: String = """
                {
                  "title": "hello world..."
                }
            """.trimIndent()
            request.setJsonEntity(data)
            client().performRequest(request)
        }

        fun queryData(indexName: String, query: String): String {
            val request = Request("GET", "/$indexName/_search?q=$query")
            val response = client().performRequest(request)
            return Streams.copyToString(InputStreamReader(response.entity.content, StandardCharsets.UTF_8))
        }

        fun refreshAnalyzer(indexName: String) {
            val request = Request(
                "POST",
                "$REFRESH_SEARCH_ANALYZER_BASE_URI/$indexName"
            )
            client().performRequest(request)
        }

        fun getSearchAnalyzerSettings(): String {
            return """
            {
                "index" : {
                    "analysis" : {
                        "analyzer" : {
                            "my_synonyms" : {
                                "tokenizer" : "whitespace",
                                "filter" : ["synonym"]
                            }
                        },
                        "filter" : {
                            "synonym" : {
                                "type" : "synonym_graph",
                                "synonyms_path" : "pacman_synonyms.txt", 
                                "updateable" : true 
                            }
                        }
                    }
                }
            }
            """.trimIndent()
        }

        fun getIndexAnalyzerSettings(): String {
            return """
            {
                "index" : {
                    "analysis" : {
                        "analyzer" : {
                            "my_synonyms" : {
                                "tokenizer" : "whitespace",
                                "filter" : ["synonym"]
                            }
                        },
                        "filter" : {
                            "synonym" : {
                                "type" : "synonym_graph",
                                "synonyms_path" : "pacman_synonyms.txt"
                            }
                        }
                    }
                }
            }
            """.trimIndent()
        }

        fun getAnalyzerMapping(): String {
            return """
            "properties": {
                    "title": {
                        "type": "text",
                        "analyzer" : "standard",
                        "search_analyzer": "my_synonyms"
                    }
                }
            """.trimIndent()
        }
    }
}
