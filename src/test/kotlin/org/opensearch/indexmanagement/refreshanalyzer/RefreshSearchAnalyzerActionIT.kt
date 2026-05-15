/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.refreshanalyzer

import org.junit.After
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
import java.util.Locale

class RefreshSearchAnalyzerActionIT : IndexManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    @After
    fun clearIndicesAfterEachTest() {
        wipeAllIndices()
    }

    @Before
    fun checkIfLocalCluster() {
        Assume.assumeTrue(isLocalTest)
    }

    fun `test index time analyzer`() {
        val buildDir = System.getProperty("buildDir")
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val indexName = "${testIndexName}_index_1"

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola")
        }

        val settings: Settings =
            Settings.builder()
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
        val indexName = "${testIndexName}_index_2"

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola")
        }

        val settings: Settings =
            Settings.builder()
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
        val indexName = "${testIndexName}_index_3"
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val buildDir = System.getProperty("buildDir")
        val aliasName = "test"
        val aliasSettings = "\"$aliasName\": {}"

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola")
        }

        val settings: Settings =
            Settings.builder()
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

    fun `test refresh succeeds with metadata write block`() {
        val buildDir = System.getProperty("buildDir")
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val indexName = "${testIndexName}_index_4"

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola")
        }

        val settings: Settings = Settings.builder()
            .loadFromSource(getSearchAnalyzerSettings(), XContentType.JSON)
            .build()
        createIndex(indexName, settings, getAnalyzerMapping())
        ingestData(indexName)

        // Verify initial synonym works
        assertTrue(queryData(indexName, "hola").contains("hello world"))

        // Add read_only block (levels: WRITE + METADATA_WRITE), simulates CCR follower block
        val addBlockRequest = Request("PUT", "/$indexName/_settings")
        addBlockRequest.setJsonEntity("""{"index.blocks.read_only": true}""")
        client().performRequest(addBlockRequest)

        // Update synonym file on disk
        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola, namaste")
        }

        // Refresh should succeed despite the METADATA_WRITE block
        refreshAnalyzer(indexName)

        // New synonym should be active in search
        assertTrue(queryData(indexName, "namaste").contains("hello world"))

        // Remove block and clean up
        val removeBlockRequest = Request("PUT", "/$indexName/_settings")
        removeBlockRequest.setJsonEntity("""{"index.blocks.read_only": false}""")
        client().performRequest(removeBlockRequest)

        for (i in 0 until numNodes) {
            deleteFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
        }
    }

    fun `test hunspell dictionary reload with reload_cached_resources`() {
        val buildDir = System.getProperty("buildDir")
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val indexName = "${testIndexName}_hunspell_1"

        // Create hunspell dictionary files on all nodes
        for (i in 0 until numNodes) {
            val hunspellDir = "$buildDir/testclusters/integTest-$i/config/analyzers/pkg-1/hunspell/en_US"
            Files.createDirectories(org.opensearch.common.io.PathUtils.get(hunspellDir))
            writeToFile("$hunspellDir/en_US.aff", "SET UTF-8\nSFX S Y 1\nSFX S 0 s .")
            writeToFile("$hunspellDir/en_US.dic", "3\napple/S\nbanana/S\nmango/S")
        }

        // Create index with hunspell analyzer (updateable=true)
        val settings: Settings = Settings.builder()
            .loadFromSource(
                """
                {
                    "index": {
                        "number_of_shards": 3,
                        "number_of_replicas": 0,
                        "analysis": {
                            "analyzer": {
                                "my_hunspell_analyzer": {
                                    "tokenizer": "standard",
                                    "filter": ["my_hunspell_filter"]
                                }
                            },
                            "filter": {
                                "my_hunspell_filter": {
                                    "type": "hunspell",
                                    "ref_path": "analyzers/pkg-1",
                                    "locale": "en_US",
                                    "updateable": true
                                }
                            }
                        }
                    }
                }
                """.trimIndent(),
                XContentType.JSON,
            )
            .build()
        createIndex(
            indexName, settings,
            """
                "properties": {
                    "content": {
                        "type": "text",
                        "analyzer": "standard",
                        "search_analyzer": "my_hunspell_analyzer"
                    }
                }
            """.trimIndent(),
        )

        // Verify initial stemming: "apples" -> "apple"
        val result1 = analyzeHunspell(indexName, "apples")
        assertTrue("Expected 'apple' in result but got: $result1", result1.contains("apple"))

        // Update dictionary on all nodes: add "kiwi"
        for (i in 0 until numNodes) {
            val hunspellDir = "$buildDir/testclusters/integTest-$i/config/analyzers/pkg-1/hunspell/en_US"
            writeToFile("$hunspellDir/en_US.dic", "4\napple/S\nbanana/S\nmango/S\nkiwi/S")
        }

        // Refresh WITHOUT reload_cached_resources - "kiwis" should NOT stem
        refreshAnalyzer(indexName)
        val result2 = analyzeHunspell(indexName, "kiwis")
        assertTrue("Expected 'kiwis' unchanged without cache reload but got: $result2", result2.contains("kiwis"))

        // Refresh WITH reload_cached_resources=true - "kiwis" should stem to "kiwi"
        val reloadRequest = Request("POST", "$REFRESH_SEARCH_ANALYZER_BASE_URI/$indexName?reload_cached_resources=true")
        client().performRequest(reloadRequest)
        val result3 = analyzeHunspell(indexName, "kiwis")
        assertTrue("Expected 'kiwi' after cache reload but got: $result3", result3.contains("\"token\":\"kiwi\""))

        // Clean up hunspell files
        for (i in 0 until numNodes) {
            val hunspellDir = "$buildDir/testclusters/integTest-$i/config/analyzers/pkg-1"
            org.opensearch.common.io.PathUtils.get(hunspellDir).toFile().deleteRecursively()
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
            val data: String =
                """
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
            val request =
                Request(
                    "POST",
                    "$REFRESH_SEARCH_ANALYZER_BASE_URI/$indexName",
                )
            client().performRequest(request)
        }

        fun getSearchAnalyzerSettings(): String = """
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

        fun getIndexAnalyzerSettings(): String = """
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

        fun getAnalyzerMapping(): String = """
                "properties": {
                        "title": {
                            "type": "text",
                            "analyzer" : "standard",
                            "search_analyzer": "my_synonyms"
                        }
                    }
        """.trimIndent()

        fun analyzeHunspell(indexName: String, text: String): String {
            val request = Request("POST", "/$indexName/_analyze")
            request.setJsonEntity("""{"analyzer": "my_hunspell_analyzer", "text": "$text"}""")
            val response = client().performRequest(request)
            return Streams.copyToString(InputStreamReader(response.entity.content, StandardCharsets.UTF_8))
        }
    }
}
