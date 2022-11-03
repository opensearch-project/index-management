/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.indexmanagement.IndexManagementIndices.Companion.HISTORY_INDEX_BASE
import org.opensearch.indexmanagement.IndexManagementIndices.Companion.HISTORY_WRITE_INDEX_ALIAS
import org.opensearch.indexmanagement.IndexManagementIndices.Companion.indexManagementMappings
import org.opensearch.indexmanagement.IndexManagementIndices.Companion.indexStateManagementHistoryMappings
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestAddPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestChangePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestExplainAction
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestRemovePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestRetryFailedManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILED_INDICES
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILURES
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.refreshanalyzer.RestRefreshSearchAnalyzerAction
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import java.util.Locale

class IndexManagementIndicesIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    /*
    * If this test fails it means you changed the config mappings
    * This test is to ensure you did not forget to increase the schema_version in the mappings _meta object
    * The schema_version is used at runtime to check if the mappings need to be updated for the index
    * Once you are sure you increased the schema_version or know it is not needed you can update the cached mappings with the new values
    * */
    fun `test config mappings schema version number`() {
        val cachedMappings = javaClass.classLoader.getResource("mappings/cached-opendistro-ism-config.json")!!.readText()
        assertEquals("I see you updated the config mappings. Did you also update the schema_version?", cachedMappings, indexManagementMappings)
    }

    /*
    * If this test fails it means you changed the history mappings
    * This test is to ensure you did not forget to increase the schema_version in the mappings _meta object
    * The schema_version is used at runtime to check if the mappings need to be updated for the index
    * Once you are sure you increased the schema_version or know it is not needed you can update the cached mappings with the new values
    * */
    fun `test history mappings schema version number`() {
        val cachedMappings = javaClass.classLoader.getResource("mappings/cached-opendistro-ism-history.json")!!.readText()
        assertEquals("I see you updated the history mappings. Did you also update the schema_version?", cachedMappings, indexStateManagementHistoryMappings)
    }

    fun `test create index management`() {
        val policy = randomPolicy()
        val policyId = OpenSearchTestCase.randomAlphaOfLength(10)
        client().makeRequest("PUT", "$POLICY_BASE_URI/$policyId", emptyMap(), policy.toHttpEntity())
        assertIndexExists(INDEX_MANAGEMENT_INDEX)
        verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, configSchemaVersion)
    }

    fun `test update management index mapping with new schema version`() {
        wipeAllODFEIndices()
        waitForPendingTasks(adminClient())
        assertIndexDoesNotExist(INDEX_MANAGEMENT_INDEX)

        val mapping = indexManagementMappings.trim().trimStart('{').trimEnd('}')
            .replace("\"schema_version\": $configSchemaVersion", "\"schema_version\": 0")

        createIndex(INDEX_MANAGEMENT_INDEX, Settings.builder().put(INDEX_HIDDEN, true).build(), mapping)
        assertIndexExists(INDEX_MANAGEMENT_INDEX)
        verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, 0)

        val policy = randomPolicy()
        val policyId = OpenSearchTestCase.randomAlphaOfLength(10)
        client().makeRequest("PUT", "$POLICY_BASE_URI/$policyId", emptyMap(), policy.toHttpEntity())

        assertIndexExists(INDEX_MANAGEMENT_INDEX)
        verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, configSchemaVersion)
    }

    fun `test update management index history mappings with new schema version`() {
        assertIndexDoesNotExist("$HISTORY_WRITE_INDEX_ALIAS?allow_no_indices=false")

        val mapping = indexStateManagementHistoryMappings.trim().trimStart('{').trimEnd('}')
            .replace("\"schema_version\": $historySchemaVersion", "\"schema_version\": 0")

        val aliases = "\"$HISTORY_WRITE_INDEX_ALIAS\": { \"is_write_index\": true }"
        createIndex("$HISTORY_INDEX_BASE-1", Settings.builder().put(INDEX_HIDDEN, true).build(), mapping, aliases)
        assertIndexExists(HISTORY_WRITE_INDEX_ALIAS)
        verifyIndexSchemaVersion(HISTORY_WRITE_INDEX_ALIAS, 0)

        val policy = createRandomPolicy()
        val (index, policyID) = createIndex("history-schema", policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(index)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // wait for the policy to initialize which will add 1 history document to the history index
        // this should update the history mappings to the new version
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index).policyID) }

        waitFor {
            assertIndexExists(HISTORY_WRITE_INDEX_ALIAS)
            verifyIndexSchemaVersion(HISTORY_WRITE_INDEX_ALIAS, historySchemaVersion)
        }
    }

    fun `test changing policy on an index that hasn't initialized yet check schema version`() {
        val policy = createRandomPolicy()
        val newPolicy = createPolicy(randomPolicy(), "new_policy", true)
        val indexName = "${testIndexName}_computer"
        val (index) = createIndex(indexName, policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(index)
        assertNull("Change policy is not null", managedIndexConfig.changePolicy)
        assertEquals("Policy id does not match", policy.id, managedIndexConfig.policyID)

        val mapping = "{" + indexManagementMappings.trimStart('{').trimEnd('}')
            .replace("\"schema_version\": $configSchemaVersion", "\"schema_version\": 0")

        val entity = StringEntity(mapping, ContentType.APPLICATION_JSON)
        client().makeRequest(
            RestRequest.Method.PUT.toString(),
            "/$INDEX_MANAGEMENT_INDEX/_mapping", emptyMap(), entity
        )

        verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, 0)

        // if we try to change policy now, it'll have no ManagedIndexMetaData yet and should succeed
        val changePolicy = ChangePolicy(newPolicy.id, null, emptyList(), false)
        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/$index", emptyMap(), changePolicy.toHttpEntity()
        )

        verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, configSchemaVersion)

        assertAffectedIndicesResponseIsEqual(mapOf(FAILURES to false, FAILED_INDICES to emptyList<Any>(), UPDATED_INDICES to 1), response.asMap())

        waitFor { assertEquals(newPolicy.id, getManagedIndexConfig(index)?.changePolicy?.policyID) }
    }

    fun `test ISM backward compatibility with opendistro`() {
        val policy = randomPolicy()
        val policyId = OpenSearchTestCase.randomAlphaOfLength(10)
        val createIndexResponse =
            client().makeRequest("PUT", "${IndexManagementPlugin.LEGACY_POLICY_BASE_URI}/$policyId", emptyMap(), policy.toHttpEntity())
        assertEquals("Create policy failed", RestStatus.CREATED, createIndexResponse.restStatus())

        val indexName = "bwc_index"
        createIndex(indexName, null)
        val addPolicyResponse = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestAddPolicyAction.LEGACY_ADD_POLICY_BASE_URI}/$indexName",
            StringEntity("{ \"policy_id\": \"$policyId\" }", ContentType.APPLICATION_JSON)
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, addPolicyResponse.restStatus())

        val changePolicyResponse = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestAddPolicyAction.LEGACY_ADD_POLICY_BASE_URI}/$indexName",
            StringEntity("{ \"policy_id\": \"$policyId\" }", ContentType.APPLICATION_JSON)
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, changePolicyResponse.restStatus())

        val retryFailedResponse = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.LEGACY_RETRY_BASE_URI}/$indexName"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, retryFailedResponse.restStatus())

        val explainResponse = client().makeRequest(
            RestRequest.Method.GET.toString(),
            "${RestExplainAction.LEGACY_EXPLAIN_BASE_URI}/$indexName"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, explainResponse.restStatus())

        val removePolicyResponse = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRemovePolicyAction.LEGACY_REMOVE_POLICY_BASE_URI}/$indexName"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, removePolicyResponse.restStatus())

        val deletePolicyResponse = client().makeRequest(
            RestRequest.Method.DELETE.toString(),
            "${IndexManagementPlugin.LEGACY_POLICY_BASE_URI}/$policyId"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, deletePolicyResponse.restStatus())

        val getPolicies = client().makeRequest(RestRequest.Method.GET.toString(), "${IndexManagementPlugin.LEGACY_POLICY_BASE_URI}")
        assertEquals("Unexpected RestStatus", RestStatus.OK, getPolicies.restStatus())
    }

    fun `test refresh search analyzer backward compatibility with opendistro`() {
        val indexName = "bwc_index"
        val settings = Settings.builder().build()
        createIndex(indexName, settings)
        val response = client().makeRequest(RestRequest.Method.POST.toString(), "${RestRefreshSearchAnalyzerAction.LEGACY_REFRESH_SEARCH_ANALYZER_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
    }

    fun `test rollup backward compatibility with opendistro`() {
        val rollup = randomRollup()
        val rollupJsonString = rollup.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()
        val createRollupResponse = client().makeRequest(
            "PUT", "${IndexManagementPlugin.LEGACY_ROLLUP_JOBS_BASE_URI}/${rollup.id}", emptyMap(),
            StringEntity(
                rollupJsonString,
                ContentType.APPLICATION_JSON
            )
        )
        assertEquals("Create rollup failed", RestStatus.CREATED, createRollupResponse.restStatus())

        val getRollupResponse = client().makeRequest("GET", "${IndexManagementPlugin.LEGACY_ROLLUP_JOBS_BASE_URI}/${rollup.id}")
        assertEquals("Get rollup failed", RestStatus.OK, getRollupResponse.restStatus())

        val explainRollupResponse = client().makeRequest("GET", "${IndexManagementPlugin.LEGACY_ROLLUP_JOBS_BASE_URI}/${rollup.id}/_explain")
        assertEquals("Explain rollup failed", RestStatus.OK, explainRollupResponse.restStatus())

        val startRollupResponse = client().makeRequest("POST", "${IndexManagementPlugin.LEGACY_ROLLUP_JOBS_BASE_URI}/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, startRollupResponse.restStatus())

        val stopRollupResponse = client().makeRequest("POST", "${IndexManagementPlugin.LEGACY_ROLLUP_JOBS_BASE_URI}/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, stopRollupResponse.restStatus())

        val deleteRollupResponse = client().makeRequest("DELETE", "${IndexManagementPlugin.LEGACY_ROLLUP_JOBS_BASE_URI}/${rollup.id}")
        assertEquals("Delete rollup failed", RestStatus.OK, deleteRollupResponse.restStatus())
    }
}
