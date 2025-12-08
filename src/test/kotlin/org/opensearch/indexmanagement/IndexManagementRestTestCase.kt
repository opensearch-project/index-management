/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.logging.log4j.LogManager
import org.junit.AfterClass
import org.junit.Before
import org.junit.rules.DisableOnDebug
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.client.WarningsHandler
import org.opensearch.common.io.PathUtils
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.Strings
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.DeprecationHandler
import org.opensearch.core.xcontent.MediaType
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import java.io.IOException
import java.nio.file.Files
import java.time.Duration
import java.time.Instant
import java.util.Date
import javax.management.MBeanServerInvocationHandler
import javax.management.ObjectName
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL

abstract class IndexManagementRestTestCase : ODFERestTestCase() {
    val configSchemaVersion = 25
    val historySchemaVersion = 7

    // Having issues with tests leaking into other tests and mappings being incorrect and they are not caught by any pending task wait check as
    // they do not go through the pending task queue. Ideally this should probably be written in a way to wait for the
    // jobs themselves to finish and gracefully shut them down.. but for now seeing if this works.
    @Before
    fun setAutoCreateIndex() {
        client().makeRequest(
            "PUT", "_cluster/settings",
            StringEntity("""{"persistent":{"action.auto_create_index":"-.opendistro-*,*"}}""", ContentType.APPLICATION_JSON),
        )
    }

    // Tests on lower resource machines are experiencing flaky failures due to attempting to force a job to
    // start before the job scheduler has registered the index operations listener. Initializing the index
    // preemptively seems to give the job scheduler time to listen to operations.
    @Before
    fun initializeManagedIndex() {
        if (!isIndexExists(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)) {
            val request = Request("PUT", "/${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}")
            var entity = "{\"settings\": " + Strings.toString(XContentType.JSON, Settings.builder().put(INDEX_HIDDEN, true).build())
            entity += ",\"mappings\" : ${IndexManagementIndices.indexManagementMappings}}"
            request.setJsonEntity(entity)
            client().performRequest(request)
        }
    }

    @Before
    fun setDebugLogLevel() {
        client().makeRequest(
            "PUT", "_cluster/settings",
            StringEntity(
                """
                {
                    "transient": {
                        "logger.org.opensearch.indexmanagement":"DEBUG",
                        "logger.org.opensearch.jobscheduler":"DEBUG"
                    }
                }
                """.trimIndent(),
                ContentType.APPLICATION_JSON,
            ),
        )
    }

    protected val isDebuggingTest = DisableOnDebug(null).isDebugging
    protected val isDebuggingRemoteCluster = System.getProperty("cluster.debug", "false")!!.toBoolean()

    protected val isLocalTest = clusterName() == "integTest"

    private fun clusterName(): String = System.getProperty("tests.clustername")

    fun Response.asMap(): Map<String, Any> = entityAsMap(this)

    protected fun Response.restStatus(): RestStatus = RestStatus.fromCode(this.statusLine.statusCode)

    protected fun isIndexExists(index: String): Boolean {
        val response = client().makeRequest("HEAD", index)
        return RestStatus.OK == response.restStatus()
    }

    protected fun assertIndexExists(index: String) {
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does not exist.", RestStatus.OK, response.restStatus())
    }

    protected fun assertIndexDoesNotExist(index: String) {
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does exist.", RestStatus.NOT_FOUND, response.restStatus())
    }

    protected fun verifyIndexSchemaVersion(index: String, expectedVersion: Int) {
        val indexMapping = client().getIndexMapping(index)
        val indexName = indexMapping.keys.toList()[0]
        val mappings = indexMapping.stringMap(indexName)?.stringMap("mappings")
        var version = 0
        if (mappings!!.containsKey("_meta")) {
            val meta = mappings.stringMap("_meta")
            if (meta!!.containsKey("schema_version")) version = meta.get("schema_version") as Int
        }
        assertEquals(expectedVersion, version)
    }

    @Suppress("UNCHECKED_CAST")
    fun Map<String, Any>.stringMap(key: String): Map<String, Any>? {
        val map = this as Map<String, Map<String, Any>>
        return map[key]
    }

    fun RestClient.getIndexMapping(index: String): Map<String, Any> {
        val response = this.makeRequest("GET", "$index/_mapping")
        assertEquals(RestStatus.OK, response.restStatus())
        return response.asMap()
    }

    /**
     * Inserts [docCount] sample documents into [index], optionally waiting [delay] milliseconds
     * in between each insertion
     */
    protected fun insertSampleData(index: String, docCount: Int, delay: Long = 0, jsonString: String = "{ \"test_field\": \"test_value\" }", routing: String? = null) {
        var endpoint = "/$index/_doc/?refresh=true"
        if (routing != null) endpoint += "&routing=$routing"
        repeat(docCount) {
            val request = Request("POST", endpoint)
            request.setJsonEntity(jsonString)
            client().performRequest(request)

            Thread.sleep(delay)
        }
    }

    protected fun insertSampleBulkData(index: String, bulkJsonString: String) {
        val request = Request("POST", "/$index/_bulk/?refresh=true")
        request.setJsonEntity(bulkJsonString)
        request.options = RequestOptions.DEFAULT.toBuilder().addHeader("content-type", "application/x-ndjson").build()
        val res = client().performRequest(request)
        assertEquals(RestStatus.OK, res.restStatus())
    }

    /**
     * Indexes 5k documents of the open NYC taxi dataset
     *
     * Example headers and document values
     * VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
     * 1,2019-01-01 00:46:40,2019-01-01 00:53:20,1,1.50,1,N,151,239,1,7,0.5,0.5,1.65,0,0.3,9.95,
     */
    protected fun generateNYCTaxiData(index: String = "nyc-taxi-data") {
        createIndex(index, Settings.EMPTY, """"properties":{"DOLocationID":{"type":"integer"},"RatecodeID":{"type":"integer"},"fare_amount":{"type":"float"},"tpep_dropoff_datetime":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"congestion_surcharge":{"type":"float"},"VendorID":{"type":"integer"},"passenger_count":{"type":"integer"},"tolls_amount":{"type":"float"},"improvement_surcharge":{"type":"float"},"trip_distance":{"type":"float"},"store_and_fwd_flag":{"type":"keyword"},"payment_type":{"type":"integer"},"total_amount":{"type":"float"},"extra":{"type":"float"},"tip_amount":{"type":"float"},"mta_tax":{"type":"float"},"tpep_pickup_datetime":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"PULocationID":{"type":"integer"}}""")
        insertSampleBulkData(index, javaClass.classLoader.getResource("data/nyc_5000.ndjson").readText())
    }

    protected fun generateMessageLogsData(index: String = "message-logs") {
        createIndex(index, Settings.EMPTY, """"properties": {"message":{"properties":{"bytes_in":{"type":"long"},"bytes_out":{"type":"long"},"plugin":{"eager_global_ordinals":true,"ignore_above":10000,"type":"keyword"},"timestamp_received":{"type":"date"}}}}""")
        insertSampleBulkData(index, javaClass.classLoader.getResource("data/message_logs.ndjson").readText())
    }

    @Suppress("UNCHECKED_CAST")
    protected fun extractFailuresFromSearchResponse(searchResponse: Response): List<Map<String, String>?>? {
        val shards = searchResponse.asMap()["_shards"] as Map<String, ArrayList<Map<String, Any>>>
        assertNotNull(shards)
        val failures = shards["failures"]
        assertNotNull(failures)
        return failures?.let {
            val result: ArrayList<Map<String, String>?>? = ArrayList()
            for (failure in it) {
                result?.add((failure as Map<String, Map<String, String>>)["reason"])
            }
            return result
        }
    }

    protected fun updateRollupStartTime(update: Rollup, desiredStartTimeMillis: Long? = null) {
        // Before updating start time of a job always make sure there are no unassigned shards that could cause the config
        // index to move to a new node and negate this forced start
        if (isMultiNode) {
            waitFor {
                try {
                    client().makeRequest("GET", "_cluster/allocation/explain")
                    fail("Expected 400 Bad Request when there are no unassigned shards to explain")
                } catch (e: ResponseException) {
                    assertEquals(RestStatus.BAD_REQUEST, e.response.restStatus())
                }
            }
        }
        val intervalSchedule = (update.jobSchedule as IntervalSchedule)
        val millis = Duration.of(intervalSchedule.interval.toLong(), intervalSchedule.unit).minusSeconds(2).toMillis()
        val startTimeMillis = desiredStartTimeMillis ?: (Instant.now().toEpochMilli() - millis)
        val waitForActiveShards = if (isMultiNode) "all" else "1"
        // TODO flaky: Add this log to confirm this update is missed by job scheduler
        // This miss is because shard remove, job scheduler deschedule on the original node and reschedule on another node
        // However the shard comes back, and job scheduler deschedule on the another node and reschedule on the original node
        // During this period, this update got missed
        // Since from the log, this happens very fast (within 0.1~0.2s), the above cluster explain may not have the granularity to catch this.
        logger.info("Update rollup start time to $startTimeMillis")
        val response =
            adminClient().makeRequest(
                "POST", "${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}/_update/${update.id}?wait_for_active_shards=$waitForActiveShards&refresh=true",
                StringEntity(
                    "{\"doc\":{\"rollup\":{\"schedule\":{\"interval\":{\"start_time\":" +
                        "\"$startTimeMillis\"}}}}}",
                    ContentType.APPLICATION_JSON,
                ),
            )

        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }

    protected fun updateTransformStartTime(update: Transform, desiredStartTimeMillis: Long? = null) {
        // Before updating start time of a job always make sure there are no unassigned shards that could cause the config
        // index to move to a new node and negate this forced start
        if (isMultiNode) {
            waitFor {
                try {
                    client().makeRequest("GET", "_cluster/allocation/explain")
                    fail("Expected 400 Bad Request when there are no unassigned shards to explain")
                } catch (e: ResponseException) {
                    assertEquals(RestStatus.BAD_REQUEST, e.response.restStatus())
                }
            }
        }
        val intervalSchedule = (update.jobSchedule as IntervalSchedule)
        val millis = Duration.of(intervalSchedule.interval.toLong(), intervalSchedule.unit).minusSeconds(2).toMillis()
        val startTimeMillis = desiredStartTimeMillis ?: (Instant.now().toEpochMilli() - millis)
        val waitForActiveShards = if (isMultiNode) "all" else "1"
        val response =
            adminClient().makeRequest(
                "POST", "${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}/_update/${update.id}?wait_for_active_shards=$waitForActiveShards",
                StringEntity(
                    "{\"doc\":{\"transform\":{\"schedule\":{\"interval\":{\"start_time\":" +
                        "\"$startTimeMillis\"}}}}}",
                    ContentType.APPLICATION_JSON,
                ),
            )

        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }

    override fun preserveIndicesUponCompletion(): Boolean = true

    companion object {
        val isMultiNode = System.getProperty("cluster.number_of_nodes", "1").toInt() > 1
        val isBWCTest = System.getProperty("tests.plugin_bwc_version", "0") != "0"
        protected val defaultKeepIndexSet = setOf(".opendistro_security")

        /**
         * We override preserveIndicesUponCompletion to true and use this function to clean up indices
         * Meant to be used in @After or @AfterClass of your feature test suite
         */
        fun wipeAllIndices(client: RestClient = adminClient(), keepIndex: Set<String> = defaultKeepIndexSet, skip: Boolean = false) {
            val logger = LogManager.getLogger(IndexManagementRestTestCase::class.java)
            if (skip) {
                logger.info("Skipping wipeAllIndices...")
                return
            }
            try {
                client.performRequest(Request("DELETE", "_data_stream/*"))
            } catch (e: ResponseException) {
                // We hit a version of ES that doesn't serialize DeleteDataStreamAction.Request#wildcardExpressionsOriginallySpecified field or
                // that doesn't support data streams so it's safe to ignore
                val statusCode = e.response.statusLine.statusCode
                if (!setOf(404, 405, 500).contains(statusCode)) {
                    throw e
                }
            }

            val response = client.performRequest(Request("GET", "/_cat/indices?format=json&expand_wildcards=all"))
            val xContentType = MediaType.fromMediaType(response.entity.contentType)
            xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.entity.content,
            ).use { parser ->
                for (index in parser.list()) {
                    val jsonObject: Map<*, *> = index as java.util.HashMap<*, *>
                    val indexName: String = jsonObject["index"] as String
                    // .opendistro_security isn't allowed to delete from cluster
                    if (!keepIndex.contains(indexName)) {
                        val request = Request("DELETE", "/$indexName")
                        // TODO: remove PERMISSIVE option after moving system index access to REST API call
                        val options = RequestOptions.DEFAULT.toBuilder()
                        options.setWarningsHandler(WarningsHandler.PERMISSIVE)
                        request.options = options.build()
                        client.performRequest(request)
                    }
                }
            }

            waitFor {
                if (!isMultiNode) {
                    waitForRunningTasks(client)
                    waitForPendingTasks(client)
                    waitForThreadPools(client)
                } else {
                    // Multi node test is not suitable to waitFor
                    // We have seen long-running write task that fails the waitFor
                    //  probably because of cluster manager - data node task not in sync
                    // So instead we just sleep 1s after wiping indices
                    Thread.sleep(1_000)
                }
            }
        }

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

            val xContentType = MediaType.fromMediaType(response.entity.contentType)
            xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.entity.content,
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

        internal interface IProxy {
            val version: String?
            var sessionId: String?

            fun getExecutionData(reset: Boolean): ByteArray?

            fun dump(reset: Boolean)

            fun reset()
        }

        /*
         * We need to be able to dump the jacoco coverage before the cluster shuts down.
         * The new internal testing framework removed some gradle tasks we were listening to,
         * to choose a good time to do it. This will dump the executionData to file after each test.
         * TODO: This is also currently just overwriting integTest.exec with the updated execData without
         *   resetting after writing each time. This can be improved to either write an exec file per test
         *   or by letting jacoco append to the file.
         * */
        @JvmStatic
        @AfterClass
        fun dumpCoverage() {
            // jacoco.dir set in esplugin-coverage.gradle, if it doesn't exist we don't
            // want to collect coverage, so we can return early
            val jacocoBuildPath = System.getProperty("jacoco.dir") ?: return
            val serverUrl = "service:jmx:rmi:///jndi/rmi://127.0.0.1:7777/jmxrmi"
            JMXConnectorFactory.connect(JMXServiceURL(serverUrl)).use { connector ->
                val proxy =
                    MBeanServerInvocationHandler.newProxyInstance(
                        connector.mBeanServerConnection,
                        ObjectName("org.jacoco:type=Runtime"),
                        IProxy::class.java,
                        false,
                    )
                proxy.getExecutionData(false)?.let {
                    val path = PathUtils.get("$jacocoBuildPath/integTest.exec")
                    Files.write(path, it)
                }
            }
        }
    }
}
