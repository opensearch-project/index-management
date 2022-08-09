/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.junit.AfterClass
import org.junit.Before
import org.junit.rules.DisableOnDebug
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.Response
import org.opensearch.client.RestClient
import org.opensearch.common.Strings
import org.opensearch.common.io.PathUtils
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.rest.RestStatus
import java.nio.file.Files
import javax.management.MBeanServerInvocationHandler
import javax.management.ObjectName
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL

abstract class IndexManagementRestTestCase : ODFERestTestCase() {

    val configSchemaVersion = 16
    val historySchemaVersion = 5

    // Having issues with tests leaking into other tests and mappings being incorrect and they are not caught by any pending task wait check as
    // they do not go through the pending task queue. Ideally this should probably be written in a way to wait for the
    // jobs themselves to finish and gracefully shut them down.. but for now seeing if this works.
    @Before
    fun setAutoCreateIndex() {
        client().makeRequest(
            "PUT", "_cluster/settings",
            StringEntity("""{"persistent":{"action.auto_create_index":"-.opendistro-*,*"}}""", ContentType.APPLICATION_JSON)
        )
    }

    // Tests on lower resource machines are experiencing flaky failures due to attempting to force a job to
    // start before the job scheduler has registered the index operations listener. Initializing the index
    // preemptively seems to give the job scheduler time to listen to operations.
    @Before
    fun initializeManagedIndex() {
        if (!indexExists(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)) {
            val request = Request("PUT", "/${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}")
            var entity = "{\"settings\": " + Strings.toString(Settings.builder().put(INDEX_HIDDEN, true).build())
            entity += ",\"mappings\" : ${IndexManagementIndices.indexManagementMappings}}"
            request.setJsonEntity(entity)
            client().performRequest(request)
        }
    }

    protected val isDebuggingTest = DisableOnDebug(null).isDebugging
    protected val isDebuggingRemoteCluster = System.getProperty("cluster.debug", "false")!!.toBoolean()
    protected val isMultiNode = System.getProperty("cluster.number_of_nodes", "1").toInt() > 1

    protected val isLocalTest = clusterName() == "integTest"
    private fun clusterName(): String {
        return System.getProperty("tests.clustername")
    }

    fun Response.asMap(): Map<String, Any> = entityAsMap(this)

    protected fun Response.restStatus(): RestStatus = RestStatus.fromCode(this.statusLine.statusCode)

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

    companion object {
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
                val proxy = MBeanServerInvocationHandler.newProxyInstance(
                    connector.mBeanServerConnection,
                    ObjectName("org.jacoco:type=Runtime"),
                    IProxy::class.java,
                    false
                )
                proxy.getExecutionData(false)?.let {
                    val path = PathUtils.get("$jacocoBuildPath/integTest.exec")
                    Files.write(path, it)
                }
            }
        }
    }
}
