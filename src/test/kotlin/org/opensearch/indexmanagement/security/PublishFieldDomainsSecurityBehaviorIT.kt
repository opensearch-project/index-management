/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.security

import org.apache.logging.log4j.LogManager
import org.opensearch.client.Request
import org.opensearch.client.RestClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.fielddomain.DateRangeFieldDomain
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata
import org.opensearch.indexmanagement.ADD_POLICY
import org.opensearch.indexmanagement.EXPLAIN_INDEX
import org.opensearch.indexmanagement.GET_INDEX_MAPPING
import org.opensearch.indexmanagement.GET_POLICY
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.MANAGED_INDEX
import org.opensearch.indexmanagement.WRITE_POLICY
import org.opensearch.indexmanagement.indexstatemanagement.action.PublishFieldDomainsAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadOnlyAction
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.fielddomain.AttemptPublishFieldDomainsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.readonly.SetReadOnlyStep
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.LockModel
import java.time.Instant
import java.util.Locale

/** Exercises publication through the ISM runner, including its injected policy-user security context. */
class PublishFieldDomainsSecurityBehaviorIT : SecurityRestTestCase() {
    fun `test restricted publisher preserves full bounds across nodes`() {
        assumeTrue("Cross-node identity propagation requires at least two nodes", isMultiNodeCluster())
        assertPublicationPreservesSearchResults(restrictDocuments = true)
    }

    fun `test DLS restricted publisher preserves global search results`() {
        assertPublicationPreservesSearchResults(restrictDocuments = true)
    }

    fun `test unrestricted publisher preserves global search results`() {
        assertPublicationPreservesSearchResults(restrictDocuments = false)
    }

    fun `test field hidden from policy user still has complete published bounds`() {
        assertPublicationPreservesSearchResults(restrictDocuments = false, hideField = true)
    }

    fun `test plugin calculation does not grant publication permission`() {
        assertPublicationPreservesSearchResults(restrictDocuments = true, allowPublication = false)
    }

    private fun assertPublicationPreservesSearchResults(
        restrictDocuments: Boolean,
        hideField: Boolean = false,
        allowPublication: Boolean = true,
    ) {
        val suffix = randomAlphaOfLength(10).lowercase(Locale.ROOT)
        val indexName = "field-domain-security-$suffix"
        val controlIndex = "field-domain-control-$suffix"
        val policyId = "field-domain-policy-$suffix"
        val user = "field-domain-user-$suffix"
        val role = "field-domain-role-$suffix"
        val password = "FieldDomain-Test-123!"
        val originalSettings = client().makeRequest("GET", "/_cluster/settings?flat_settings=true").asMap()
            .stringMap("persistent").orEmpty()

        try {
            updateClusterSetting(ManagedIndexSettings.JITTER.key, "0.0", false)
            updateClusterSetting(PRUNING_ENABLED, "false", false)
            updateClusterSetting(PRUNING_MIN_SHARDS, "1", false)
            updateClusterSetting(PRUNING_FIELDS, "[\"$FIELD\"]", false)
            createUser(user, password)
            createPublisherRole(role, indexName, restrictDocuments, hideField, allowPublication)
            assignRoleToUsers(role, listOf(user))

            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), user, password)
                .setSocketTimeout(60000)
                .setConnectionRequestTimeout(180000)
                .build().use { publisher ->
                    val mapping = """"properties":{"tenant":{"type":"keyword"},"$FIELD":{"type":"date"}}"""
                    val multiNode = isMultiNodeCluster()
                    val allocationSettings = if (multiNode) {
                        Settings.builder().put("index.routing.allocation.total_shards_per_node", 1).build()
                    } else {
                        Settings.EMPTY
                    }
                    createIndex(index = indexName, policyID = null, replicas = "0", shards = "2", mapping = mapping, settings = allocationSettings)
                    createIndex(index = controlIndex, policyID = null, replicas = "0", shards = "1", mapping = mapping)
                    if (multiNode) assertShardsOnDifferentNodes(indexName)
                    val (firstRouting, secondRouting) = routingForDifferentShards(indexName)
                    insertSampleData(indexName, 1, jsonString = document("hidden", JANUARY), routing = firstRouting)
                    insertSampleData(indexName, 1, jsonString = document("visible", JUNE), routing = secondRouting)
                    // This matching index prevents the all-pruned fallback from masking an incorrect domain.
                    insertSampleData(controlIndex, 1, jsonString = document("visible", JANUARY))

                    assertEquals(2L, searchCount(client(), indexName, MATCH_ALL))
                    assertEquals(if (restrictDocuments) 1L else 2L, searchCount(publisher, indexName, MATCH_ALL))
                    if (hideField) assertEquals(0L, searchCount(publisher, indexName, FIELD_EXISTS))

                    val state = State(
                        "publish",
                        listOf(
                            ReadOnlyAction(0),
                            PublishFieldDomainsAction(listOf(FieldDomainConfig(FIELD, DateRangeFieldDomain.TYPE)), 1),
                        ),
                        emptyList(),
                    )
                    val policy = Policy(
                        id = policyId,
                        description = "Verify field-domain publication with policy-user permissions",
                        schemaVersion = 1L,
                        lastUpdatedTime = Instant.now(),
                        errorNotification = null,
                        defaultState = state.name,
                        states = listOf(state),
                    )
                    createPolicy(policy, policyId, client = publisher)
                    addPolicyToIndex(indexName, policyId, RestStatus.OK, publisher)
                    val managedIndex = getExistingManagedIndexConfig(indexName)
                    assertEquals(user, managedIndex.policy.user?.name)
                    assertTrue(managedIndex.policy.user!!.roles.contains(role))

                    updateManagedIndexConfigStartTime(managedIndex)
                    waitFor {
                        assertEquals(policyId, getExplainManagedIndexMetaData(indexName).policyID)
                        assertJobLockReleased(managedIndex.id)
                    }
                    updateManagedIndexConfigStartTime(managedIndex)
                    waitFor {
                        val settings = client().makeRequest("GET", "/$indexName/_settings?flat_settings=true").asMap()
                        assertEquals("true", settings.stringMap(indexName)?.stringMap("settings")?.get("index.blocks.write"))
                        // The write block is visible before the runner persists step completion.
                        assertEquals(SetReadOnlyStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message"))
                        assertJobLockReleased(managedIndex.id)
                    }
                    updateManagedIndexConfigStartTime(managedIndex)
                    if (!allowPublication) {
                        waitFor {
                            val metadata = getExplainManagedIndexMetaData(indexName)
                            assertEquals(AttemptPublishFieldDomainsStep.getFailedPublishMessage(indexName), metadata.info?.get("message"))
                            assertTrue(metadata.info?.get("cause").toString().contains("no permissions for [indices:admin/field_domains/put]"))
                        }
                        assertNull("Unauthorized publication must not write metadata", publishedDomain(indexName))
                        assertEquals(1L, searchCount(publisher, indexName, MATCH_ALL))
                        return@use
                    }
                    waitFor {
                        val metadata = getExplainManagedIndexMetaData(indexName)
                        assertEquals(
                            "Publication status: ${metadata.info}",
                            AttemptPublishFieldDomainsStep.getSuccessMessage(indexName, 1),
                            metadata.info?.get("message"),
                        )
                    }

                    val domain = publishedDomain(indexName)
                    assertNotNull("No published field-domain metadata", domain)
                    val min = domain!!["fields.$FIELD.min"]
                    val max = domain["fields.$FIELD.max"]
                    assertEquals("true", domain["fields.$FIELD.finalized"])
                    assertEquals("milliseconds", domain["fields.$FIELD.resolution"])

                    val baseline = searchCount(client(), "$indexName,$controlIndex", JANUARY_QUERY)
                    assertEquals(2L, baseline)
                    updateClusterSetting(PRUNING_ENABLED, "true", false)
                    val withPruning = searchCount(client(), "$indexName,$controlIndex", JANUARY_QUERY)
                    LogManager.getLogger(javaClass).info(
                        "Field-domain security reproduction: restricted={}, min={}, max={}, baseline={}, pruning={}",
                        restrictDocuments, min, max, baseline, withPruning,
                    )
                    assertEquals(
                        "Published min=$min, max=$max, restricted=$restrictDocuments; pruning must preserve all authorized hits",
                        baseline,
                        withPruning,
                    )
                    assertEquals(Instant.parse(JANUARY).toEpochMilli().toString(), min)
                    assertEquals(Instant.parse(JUNE).toEpochMilli().toString(), max)
                    assertEquals(if (restrictDocuments) 1L else 2L, searchCount(publisher, indexName, MATCH_ALL))
                    if (hideField) assertEquals(0L, searchCount(publisher, indexName, FIELD_EXISTS))
                }
        } finally {
            val restored = listOf(PRUNING_ENABLED, PRUNING_MIN_SHARDS, PRUNING_FIELDS, ManagedIndexSettings.JITTER.key)
                .associateWith { originalSettings[it] }
            val request = Request("PUT", "/_cluster/settings")
            request.setJsonEntity(
                XContentFactory.jsonBuilder().use { builder ->
                    builder.startObject().field("persistent", restored).endObject()
                    BytesReference.bytes(builder).utf8ToString()
                },
            )
            client().performRequest(request)
            deleteIndexByName(indexName)
            deleteIndexByName(controlIndex)
            deleteIndexByName(INDEX_MANAGEMENT_INDEX)
            executeRequest(Request("DELETE", "/_plugins/_security/api/rolesmapping/$role"), client = client())
            deleteUser(user)
            deleteRole(role)
        }
    }

    private fun assertJobLockReleased(jobId: String) {
        // Persisted metadata precedes lock release; wait before forcing the next run.
        val lockId = LockModel.generateLockId(INDEX_MANAGEMENT_INDEX, jobId)
        val lock = adminClient().makeRequest("GET", "/.opendistro-job-scheduler-lock/_doc/$lockId").asMap()
        assertEquals("Job [$jobId] must release its lock before the next run", true, lock.stringMap("_source")?.get(LockModel.RELEASED))
    }

    private fun publishedDomain(indexName: String): Map<String, Any>? {
        val metadata = client().makeRequest("GET", "/_cluster/state/metadata/$indexName").asMap()
        return metadata.stringMap("metadata")?.stringMap("indices")?.stringMap(indexName)
            ?.stringMap(IndexFieldDomainMetadata.CUSTOM_KEY)
    }

    private fun isMultiNodeCluster(): Boolean = System.getProperty("cluster.number_of_nodes", "1").toInt() > 1

    private fun assertShardsOnDifferentNodes(indexName: String) {
        waitFor {
            val groups = client().makeRequest("GET", "/$indexName/_search_shards").asMap()["shards"] as List<*>
            val primaries = groups.map { (it as List<*>).single() as Map<*, *> }
            assertEquals(2, primaries.size)
            assertTrue(primaries.all { it["primary"] == true && it["state"] == "STARTED" && it["node"] != null })
            assertEquals("Boundary searches must reach two different nodes", 2, primaries.map { it["node"] }.toSet().size)
        }
    }

    private fun routingForDifferentShards(indexName: String): List<String> {
        val routingByShard = mutableMapOf<Int, String>()
        for (candidate in 0 until 100) {
            val groups = client().makeRequest("GET", "/$indexName/_search_shards?routing=$candidate").asMap()["shards"] as List<*>
            val shard = (groups.single() as List<*>).single() as Map<*, *>
            routingByShard[(shard["shard"] as Number).toInt()] = candidate.toString()
            if (routingByShard.size == 2) return routingByShard.values.toList()
        }
        throw AssertionError("Could not find routing values for both shards of [$indexName]")
    }

    private fun createPublisherRole(role: String, indexName: String, restrictDocuments: Boolean, hideField: Boolean, allowPublication: Boolean) {
        val dls = if (restrictDocuments) """"dls":"{\"term\":{\"tenant\":\"visible\"}}",""" else ""
        val fls = if (hideField) """"fls":["tenant"],""" else ""
        // Security currently resolves the publication request to all indices. Keep this broader grant
        // limited to publication, with the same DLS restriction, so it cannot grant unrestricted reads.
        val publication = if (allowPublication) {
            """, {"index_patterns":["*"],$dls $fls "allowed_actions":["indices:admin/field_domains/put"]}"""
        } else {
            ""
        }
        val request = Request("PUT", "/_plugins/_security/api/roles/$role")
        request.setJsonEntity(
            """
            {
              "cluster_permissions": ["$WRITE_POLICY", "$GET_POLICY", "$ADD_POLICY", "$EXPLAIN_INDEX"],
              "index_permissions": [{
                "index_patterns": ["$indexName"],
                $dls
                $fls
                "allowed_actions": ["read", "$MANAGED_INDEX", "$GET_INDEX_MAPPING",
                  "indices:admin/settings/update", "indices:admin/refresh*"]
              }$publication],
              "tenant_permissions": []
            }
            """.trimIndent(),
        )
        executeRequest(request, RestStatus.CREATED, client())
    }

    private fun searchCount(searchClient: RestClient, indices: String, query: String): Long {
        val request = Request("POST", "/$indices/_search")
        request.addParameter("request_cache", "false")
        request.addParameter("allow_partial_search_results", "false")
        request.setJsonEntity("""{"size":0,"track_total_hits":true,"query":$query}""")
        val response = searchClient.performRequest(request).asMap()
        assertEquals(false, response["timed_out"])
        assertEquals(0, response.stringMap("_shards")?.get("failed"))
        return (response.stringMap("hits")!!.stringMap("total")!!["value"] as Number).toLong()
    }

    private fun document(tenant: String, timestamp: String) = """{"tenant":"$tenant","$FIELD":"$timestamp"}"""

    companion object {
        private const val FIELD = "@timestamp"
        private const val JANUARY = "2026-01-10T00:00:00Z"
        private const val JUNE = "2026-06-10T00:00:00Z"
        private const val MATCH_ALL = """{"match_all":{}}"""
        private const val FIELD_EXISTS = """{"exists":{"field":"@timestamp"}}"""
        private const val JANUARY_QUERY = """{"range":{"@timestamp":{"gte":"2026-01-01","lt":"2026-02-01"}}}"""
        private const val PRUNING_ENABLED = "search.index_pruning.enabled"
        private const val PRUNING_MIN_SHARDS = "search.index_pruning.min_shards"
        private const val PRUNING_FIELDS = "search.index_pruning.fields"
    }
}
