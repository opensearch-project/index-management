/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILED_INDICES
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILURES
import org.opensearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestStatus

class RestRemovePolicyActionIT : IndexStateManagementRestTestCase() {

    fun `test missing indices`() {
        try {
            client().makeRequest(POST.toString(), RestRemovePolicyAction.REMOVE_POLICY_BASE_URI)
            fail("Expected a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "illegal_argument_exception", "reason" to "Missing indices")
                    ),
                    "type" to "illegal_argument_exception",
                    "reason" to "Missing indices"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test closed index`() {
        val index = "movies"
        val policy = createRandomPolicy()
        createIndex(index, policy.id)
        closeIndex(index)

        val response = client().makeRequest(
            POST.toString(),
            "${RestRemovePolicyAction.REMOVE_POLICY_BASE_URI}/$index"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to index,
                    "index_uuid" to getUuid(index),
                    "reason" to "This index is closed"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)
    }

    fun `test index without policy`() {
        val index = "movies"
        createRandomPolicy()
        createIndex(index, null)

        val response = client().makeRequest(
            POST.toString(),
            "${RestRemovePolicyAction.REMOVE_POLICY_BASE_URI}/$index"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to index,
                    "index_uuid" to getUuid(index),
                    "reason" to "This index does not have a policy to remove"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)
    }

    fun `test index list`() {
        val indexOne = "movies_1"
        val indexTwo = "movies_2"
        val policy = createRandomPolicy()
        createIndex(indexOne, policy.id)
        createIndex(indexTwo, null)

        closeIndex(indexOne)

        val response = client().makeRequest(
            POST.toString(),
            "${RestRemovePolicyAction.REMOVE_POLICY_BASE_URI}/$indexOne,$indexTwo"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexOne,
                    "index_uuid" to getUuid(indexOne),
                    "reason" to "This index is closed"
                ),
                mapOf(
                    "index_name" to indexTwo,
                    "index_uuid" to getUuid(indexTwo),
                    "reason" to "This index does not have a policy to remove"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)
    }

    fun `test index pattern`() {
        val indexPattern = "movies"
        val indexOne = "movies_1"
        val indexTwo = "movies_2"
        val indexThree = "movies_3"
        val policy = createRandomPolicy()
        createIndex(indexOne, policy.id)
        createIndex(indexTwo, null)
        createIndex(indexThree, policy.id)

        closeIndex(indexOne)

        val response = client().makeRequest(
            POST.toString(),
            "${RestRemovePolicyAction.REMOVE_POLICY_BASE_URI}/$indexPattern*"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            UPDATED_INDICES to 1,
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexOne,
                    "index_uuid" to getUuid(indexOne),
                    "reason" to "This index is closed"
                ),
                mapOf(
                    "index_name" to indexTwo,
                    "index_uuid" to getUuid(indexTwo),
                    "reason" to "This index does not have a policy to remove"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)

        // Check if indexThree had policy removed
        waitFor {
            assertEquals(null, getPolicyIDOfManagedIndex(indexThree))
        }
    }

    fun `test remove policy on read only index update auto_manage setting`() {
        val index1 = "read_only_index"
        val index2 = "read_only_allow_delete_index"
        val index3 = "normal_index"
        val index4 = "auto_manage_false_index"
        val indexPattern = "*index"
        val policy = createRandomPolicy()
        createIndex(index1, policy.id)
        createIndex(index2, policy.id)
        createIndex(index3, policy.id)
        createIndex(index4, policy.id)
        updateIndexSetting(index1, IndexMetadata.SETTING_READ_ONLY, "true")
        updateIndexSetting(index2, IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, "true")
        updateIndexSetting(index4, ManagedIndexSettings.AUTO_MANAGE.key, "false")

        val response = client().makeRequest(
            POST.toString(),
            "${RestRemovePolicyAction.REMOVE_POLICY_BASE_URI}/$indexPattern"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            UPDATED_INDICES to 4,
            FAILURES to false,
            FAILED_INDICES to emptyList<Any>()
        )
        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)

        waitFor {
            assertEquals("auto manage setting not false after removing policy for index $index1", false, getIndexAutoManageSetting(index1))
            assertEquals("read only allow delete setting changed after removing policy for index $index1", null, getIndexReadOnlyAllowDeleteSetting(index1))
            assertEquals("auto manage setting not false after removing policy for index $index2", false, getIndexAutoManageSetting(index2))
            assertEquals("read only setting changed after removing policy for index $index2", null, getIndexReadOnlySetting(index2))
            assertEquals("auto manage setting not false after removing policy for index $index3", false, getIndexAutoManageSetting(index3))
            assertEquals("read only setting changed after removing policy for index $index3", null, getIndexReadOnlySetting(index3))
            assertEquals("read only allow delete setting changed after removing policy for index $index3", null, getIndexReadOnlyAllowDeleteSetting(index3))
            assertEquals("auto manage setting not false after removing policy for index $index4", false, getIndexAutoManageSetting(index3))
            assertEquals("read only setting changed after removing policy for index $index4", null, getIndexReadOnlySetting(index3))
            assertEquals("read only allow delete setting changed after removing policy for index $index4", null, getIndexReadOnlyAllowDeleteSetting(index3))
        }

        // otherwise, test cleanup cannot delete this index
        updateIndexSetting(index1, IndexMetadata.SETTING_READ_ONLY, "false")
    }
}
