/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.apache.http.HttpHost
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction
import org.opensearch.client.Request
import org.opensearch.client.Response
import org.opensearch.client.RestClient
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
import java.util.Date

abstract class ODFERestTestCase : OpenSearchRestTestCase() {

    fun isHttps(): Boolean = System.getProperty("https", "false")!!.toBoolean()

    fun securityEnabled(): Boolean = System.getProperty("security", "false")!!.toBoolean()

    override fun getProtocol(): String = if (isHttps()) "https" else "http"

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        protected fun waitForRunningTasks(client: RestClient) {
            val runningTasks: MutableSet<String> = runningTasks(client.performRequest(Request("GET", "/_tasks?detailed")))
            if (runningTasks.isEmpty()) {
                return
            }
            val stillRunning = ArrayList<String>(runningTasks)
            fail("${Date()}: There are still tasks running after this test that might break subsequent tests: \n${stillRunning.joinToString("\n")}.")
        }

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
                    // Ignore the task list API - it doesn't count against us
                    if (task["action"] == ListTasksAction.NAME || task["action"] == ListTasksAction.NAME + "[n]") continue
                    // runningTasks.add(task["action"].toString() + " | " + task["description"].toString())
                    runningTasks.add(task.toString())
                }
            }
            return runningTasks
        }

        @JvmStatic
        protected fun waitForThreadPools(client: RestClient) {
            val response = client.performRequest(Request("GET", "/_cat/thread_pool?format=json"))

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
