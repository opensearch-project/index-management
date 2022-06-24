/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.apache.http.HttpHost
import org.junit.After
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.Response
import org.opensearch.client.RestClient
import org.opensearch.client.WarningsHandler
import org.opensearch.common.io.PathUtils
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.DeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_ENABLED
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_FILEPATH
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_KEYPASSWORD
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_PASSWORD
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_PEMCERT_FILEPATH
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.io.IOException

abstract class ODFERestTestCase : OpenSearchRestTestCase() {

    fun isHttps(): Boolean = System.getProperty("https", "false")!!.toBoolean()

    fun securityEnabled(): Boolean = System.getProperty("security", "false")!!.toBoolean()

    override fun getProtocol(): String = if (isHttps()) "https" else "http"

    @Suppress("UNCHECKED_CAST")
    @Throws(IOException::class)
    private fun runningTasks(response: Response): MutableSet<String> {
        val runningTasks: MutableSet<String> = HashSet()
        val nodes = entityAsMap(response)["nodes"] as Map<String, Any>?
        for ((_, value) in nodes!!) {
            val nodeInfo = value as Map<String, Any>
            val nodeTasks = nodeInfo["tasks"] as Map<String, Any>?
            for ((_, value1) in nodeTasks!!) {
                val task = value1 as Map<String, Any>
                runningTasks.add(task["action"].toString())
            }
        }
        return runningTasks
    }

    @After
    fun waitForCleanup() {
        waitFor {
            waitForRunningTasks()
            waitForThreadPools()
            waitForPendingTasks(adminClient())
        }
    }

    @Throws(IOException::class)
    private fun waitForRunningTasks() {
        val runningTasks: MutableSet<String> = runningTasks(adminClient().performRequest(Request("GET", "/_tasks")))
        // Ignore the task list API - it doesn't count against us
        runningTasks.remove(ListTasksAction.NAME)
        runningTasks.remove(ListTasksAction.NAME + "[n]")
        if (runningTasks.isEmpty()) {
            return
        }
        val stillRunning = ArrayList<String>(runningTasks)
        fail("There are still tasks running after this test that might break subsequent tests $stillRunning.")
    }

    private fun waitForThreadPools() {
        waitFor {
            val response = client().performRequest(Request("GET", "/_cat/thread_pool?format=json"))

            val xContentType = XContentType.fromMediaType(response.entity.contentType.value)
            xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.entity.content
            ).use { parser ->
                for (index in parser.list()) {
                    val jsonObject: Map<*, *> = index as java.util.HashMap<*, *>
                    val active = (jsonObject["active"] as String).toInt()
                    val queue = (jsonObject["queue"] as String).toInt()
                    val name = jsonObject["name"]
                    val trueActive = if (name == "management") active - 1 else active
                    if (trueActive > 0 || queue > 0) {
                        fail("Still active threadpools in cluster: $jsonObject")
                    }
                }
            }
        }
    }

    open fun preserveODFEIndicesAfterTest(): Boolean = false

    @Throws(IOException::class)
    open fun wipeAllODFEIndices() {
        if (preserveODFEIndicesAfterTest()) return

        // Delete all data stream indices
        client().performRequest(Request("DELETE", "/_data_stream/*"))

        // Delete all indices
        val response = client().performRequest(Request("GET", "/_cat/indices?format=json&expand_wildcards=all"))

        val xContentType = XContentType.fromMediaType(response.entity.contentType.value)
        xContentType.xContent().createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            response.entity.content
        ).use { parser ->
            for (index in parser.list()) {
                val jsonObject: Map<*, *> = index as java.util.HashMap<*, *>
                val indexName: String = jsonObject["index"] as String
                // .opendistro_security isn't allowed to delete from cluster
                if (".opendistro_security" != indexName) {
                    val request = Request("DELETE", "/$indexName")
                    // TODO: remove PERMISSIVE option after moving system index access to REST API call
                    val options = RequestOptions.DEFAULT.toBuilder()
                    options.setWarningsHandler(WarningsHandler.PERMISSIVE)
                    request.options = options.build()
                    adminClient().performRequest(request)
                }
            }
        }
    }
    /**
     * Returns the REST client settings used for super-admin actions like cleaning up after the test has completed.
     */
    override fun restAdminSettings(): Settings {
        return Settings
            .builder()
            .put("http.port", 9200)
            .put(OPENSEARCH_SECURITY_SSL_HTTP_ENABLED, isHttps())
            .put(OPENSEARCH_SECURITY_SSL_HTTP_PEMCERT_FILEPATH, "sample.pem")
            .put(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_FILEPATH, "test-kirk.jks")
            .put(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_PASSWORD, "changeit")
            .put(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_KEYPASSWORD, "changeit")
            .build()
    }

    @Throws(IOException::class)
    override fun buildClient(settings: Settings, hosts: Array<HttpHost>): RestClient {
        if (isHttps()) {
            val keystore = settings.get(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_FILEPATH)
            return when (keystore != null) {
                true -> {
                    // create adminDN (super-admin) client
                    val uri = javaClass.classLoader.getResource("security/sample.pem").toURI()
                    val configPath = PathUtils.get(uri).parent.toAbsolutePath()
                    SecureRestClientBuilder(settings, configPath).setSocketTimeout(60000).build()
                }
                false -> {
                    // create client with passed user
                    val userName = System.getProperty("user")
                    val password = System.getProperty("password")
                    SecureRestClientBuilder(hosts, isHttps(), userName, password).setSocketTimeout(60000).build()
                }
            }
        } else {
            val builder = RestClient.builder(*hosts)
            configureClient(builder, settings)
            builder.setStrictDeprecationMode(true)
            return builder.build()
        }
    }
}
