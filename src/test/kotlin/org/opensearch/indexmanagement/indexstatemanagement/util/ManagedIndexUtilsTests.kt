/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.util

import org.opensearch.action.delete.DeleteRequest
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.destination.message.LegacyBaseMessage
import org.opensearch.commons.destination.message.LegacyCustomWebhookMessage
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Conditions
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomClusterStateManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomSweptManagedIndexConfig
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class ManagedIndexUtilsTests : OpenSearchTestCase() {

    fun `test create managed index request`() {
        val index = randomAlphaOfLength(10)
        val uuid = randomAlphaOfLength(10)
        val policyID = randomAlphaOfLength(10)
        val createRequest = managedIndexConfigIndexRequest(index, uuid, policyID, 5, jobJitter = 0.0)

        assertNotNull("IndexRequest not created", createRequest)
        assertEquals("Incorrect ism index used in request", INDEX_MANAGEMENT_INDEX, createRequest.index())
        assertEquals("Incorrect uuid used as document id on request", uuid, createRequest.id())

        val source = createRequest.source()
        val managedIndexConfig = contentParser(source).parseWithType(parse = ManagedIndexConfig.Companion::parse)
        assertEquals("Incorrect index on ManagedIndexConfig source", index, managedIndexConfig.index)
        assertEquals("Incorrect name on ManagedIndexConfig source", index, managedIndexConfig.name)
        assertEquals("Incorrect index uuid on ManagedIndexConfig source", uuid, managedIndexConfig.indexUuid)
        assertEquals("Incorrect policy_id on ManagedIndexConfig source", policyID, managedIndexConfig.policyID)
    }

    fun `test delete managed index request`() {
        val uuid = randomAlphaOfLength(10)
        val deleteRequest = deleteManagedIndexRequest(uuid)

        assertNotNull("deleteRequest not created", deleteRequest)
        assertEquals("Incorrect ism index used in request", INDEX_MANAGEMENT_INDEX, deleteRequest.index())
        assertEquals("Incorrect uuid used as document id on request", uuid, deleteRequest.id())
    }

    @Suppress("UNCHECKED_CAST")
    fun `test update managed index request`() {
        val index = randomAlphaOfLength(10)
        val uuid = randomAlphaOfLength(10)
        val policyID = randomAlphaOfLength(10)
        val sweptManagedIndexConfig = SweptManagedIndexConfig(
            index = index, uuid = uuid, policyID = policyID,
            primaryTerm = 1, seqNo = 1, changePolicy = randomChangePolicy(policyID = policyID), policy = null
        )
        val updateRequest = updateManagedIndexRequest(sweptManagedIndexConfig)

        assertNotNull("UpdateRequest not created", updateRequest)
        assertEquals("Incorrect ism index used in request", INDEX_MANAGEMENT_INDEX, updateRequest.index())
        assertEquals("Incorrect uuid used as document id on request", uuid, updateRequest.id())

        val source = updateRequest.doc().sourceAsMap()
        logger.info("source is $source")
        assertEquals(
            "Incorrect policy_id added to change_policy", sweptManagedIndexConfig.policyID,
            ((source["managed_index"] as Map<String, Any>)["change_policy"] as Map<String, String>)["policy_id"]
        )
    }

    fun `test get delete managed index requests`() {
        val clusterConfigToCreate = randomClusterStateManagedIndexConfig(policyID = "some_policy")
        val sweptConfigToDelete = randomSweptManagedIndexConfig(policyID = "delete_me")

        val clusterConfigToUpdate = randomClusterStateManagedIndexConfig(policyID = "update_me")
        val sweptConfigToBeUpdated = randomSweptManagedIndexConfig(
            index = clusterConfigToUpdate.index,
            uuid = clusterConfigToUpdate.uuid, policyID = "to_something_new", seqNo = 5, primaryTerm = 17
        )

        val clusterConfigBeingUpdated = randomClusterStateManagedIndexConfig(policyID = "updating")
        val sweptConfigBeingUpdated = randomSweptManagedIndexConfig(
            index = clusterConfigBeingUpdated.index,
            uuid = clusterConfigBeingUpdated.uuid, policyID = "to_something_new", seqNo = 5, primaryTerm = 17,
            changePolicy = randomChangePolicy("updating")
        )

        val clusterConfig = randomClusterStateManagedIndexConfig(policyID = "do_nothing")
        val sweptConfig = randomSweptManagedIndexConfig(
            index = clusterConfig.index,
            uuid = clusterConfig.uuid, policyID = clusterConfig.policyID, seqNo = 5, primaryTerm = 17
        )

        val managedIndicesToDelete = getManagedIndicesToDelete(
            listOf(clusterConfig.uuid, clusterConfigToUpdate.uuid, clusterConfigBeingUpdated.uuid, clusterConfigToCreate.uuid),
            listOf(sweptConfig.uuid, sweptConfigToDelete.uuid, sweptConfigToBeUpdated.uuid, sweptConfigBeingUpdated.uuid)
        )
        val requests = managedIndicesToDelete.map { deleteManagedIndexRequest(it) }

        assertEquals("Too many requests", 1, requests.size)
        val request = requests.first()
        assertEquals("Incorrect uuid used as document id on request", sweptConfigToDelete.uuid, request.id())
        assertTrue("Incorrect request type", request is DeleteRequest)
    }

    fun `test get swept managed index search request`() {
        val searchRequest = getSweptManagedIndexSearchRequest()

        val builder = searchRequest.source()
        val indices = searchRequest.indices().toList()
        assertTrue("Does not return seqNo and PrimaryTerm", builder.seqNoAndPrimaryTerm())
        assertEquals("Wrong index being searched", listOf(INDEX_MANAGEMENT_INDEX), indices)
    }

    fun `test rollover action config evaluate conditions`() {
        val noConditionsConfig = RolloverAction(minSize = null, minDocs = null, minAge = null, minPrimaryShardSize = null, index = 0)
        assertTrue(
            "No conditions should always pass",
            noConditionsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(0), numDocs = 0, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )
        assertTrue(
            "No conditions should always pass",
            noConditionsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(100), numDocs = 5, indexSize = ByteSizeValue(5), primaryShardSize = ByteSizeValue(5))
        )
        assertTrue(
            "No conditions should always pass",
            noConditionsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(6000), numDocs = 5, indexSize = ByteSizeValue(5), primaryShardSize = ByteSizeValue(5))
        )

        val minSizeConfig = RolloverAction(minSize = ByteSizeValue(5), minDocs = null, minAge = null, minPrimaryShardSize = null, index = 0)
        assertFalse(
            "Less bytes should not pass",
            minSizeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )
        assertTrue(
            "Equal bytes should pass",
            minSizeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue(5), primaryShardSize = ByteSizeValue(5))
        )
        assertTrue(
            "More bytes should pass",
            minSizeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue(10), primaryShardSize = ByteSizeValue(10))
        )

        val minPrimarySizeConfig = RolloverAction(minSize = null, minDocs = null, minAge = null, minPrimaryShardSize = ByteSizeValue(5), index = 0)
        assertFalse(
            "Less primary bytes should not pass",
            minPrimarySizeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )
        assertTrue(
            "Equal primary bytes should pass",
            minPrimarySizeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue(5), primaryShardSize = ByteSizeValue(5))
        )
        assertTrue(
            "More primary bytes should pass",
            minPrimarySizeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue(10), primaryShardSize = ByteSizeValue(10))
        )

        val minDocsConfig = RolloverAction(minSize = null, minDocs = 5, minAge = null, minPrimaryShardSize = null, index = 0)
        assertFalse(
            "Less docs should not pass",
            minDocsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )
        assertTrue(
            "Equal docs should pass",
            minDocsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 5, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )
        assertTrue(
            "More docs should pass",
            minDocsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 10, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )

        val minAgeConfig = RolloverAction(minSize = null, minDocs = null, minAge = TimeValue.timeValueSeconds(5), minPrimaryShardSize = null, index = 0)
        assertFalse(
            "Index age that is too young should not pass",
            minAgeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )
        assertTrue(
            "Index age that is older should pass",
            minAgeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(10000), numDocs = 0, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )

        val multiConfig = RolloverAction(minSize = ByteSizeValue(1), minDocs = 1, minAge = TimeValue.timeValueSeconds(5), minPrimaryShardSize = ByteSizeValue(1), index = 0)
        assertFalse(
            "No conditions met should not pass",
            multiConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(0), numDocs = 0, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )
        assertTrue(
            "Multi condition, age should pass",
            multiConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(10000), numDocs = 0, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )
        assertTrue(
            "Multi condition, docs should pass",
            multiConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(0), numDocs = 2, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue.ZERO)
        )
        assertTrue(
            "Multi condition, size should pass",
            multiConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(0), numDocs = 0, indexSize = ByteSizeValue(2), primaryShardSize = ByteSizeValue.ZERO)
        )

        assertTrue(
            "Multi condition, primary size should pass",
            multiConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(0), numDocs = 0, indexSize = ByteSizeValue.ZERO, primaryShardSize = ByteSizeValue(2))
        )
    }

    fun `test transition evaluate conditions`() {
        val emptyTransition = Transition(stateName = "some_state", conditions = null)
        assertTrue(
            "No conditions should pass",
            emptyTransition
                .evaluateConditions(indexCreationDate = Instant.now(), numDocs = null, indexSize = null, transitionStartTime = Instant.now(), rolloverDate = null)
        )

        val timeTransition = Transition(
            stateName = "some_state",
            conditions = Conditions(indexAge = TimeValue.timeValueSeconds(5))
        )
        assertFalse(
            "Index age that is too young should not pass",
            timeTransition
                .evaluateConditions(indexCreationDate = Instant.now(), numDocs = null, indexSize = null, transitionStartTime = Instant.now(), rolloverDate = null)
        )
        assertTrue(
            "Index age that is older should pass",
            timeTransition
                .evaluateConditions(indexCreationDate = Instant.now().minusSeconds(10), numDocs = null, indexSize = null, transitionStartTime = Instant.now(), rolloverDate = null)
        )
        assertFalse(
            "Index age that is -1L should not pass",
            timeTransition
                .evaluateConditions(indexCreationDate = Instant.ofEpochMilli(-1L), numDocs = null, indexSize = null, transitionStartTime = Instant.now(), rolloverDate = null)
        )

        val rolloverTimeTransition = Transition(
            stateName = "some_state",
            conditions = Conditions(rolloverAge = TimeValue.timeValueSeconds(5))
        )
        assertFalse(
            "Rollover age that is too young should not pass",
            rolloverTimeTransition
                .evaluateConditions(indexCreationDate = Instant.now(), numDocs = null, indexSize = null, transitionStartTime = Instant.now(), rolloverDate = Instant.now())
        )
        assertTrue(
            "Rollover age that is older should pass",
            rolloverTimeTransition
                .evaluateConditions(indexCreationDate = Instant.now().minusSeconds(10), numDocs = null, indexSize = null, transitionStartTime = Instant.now(), rolloverDate = Instant.now().minusSeconds(10))
        )
        assertFalse(
            "Rollover age that is null should not pass",
            rolloverTimeTransition
                .evaluateConditions(indexCreationDate = Instant.ofEpochMilli(-1L), numDocs = null, indexSize = null, transitionStartTime = Instant.now(), rolloverDate = null)
        )
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(
            xContentRegistry(), LoggingDeprecationHandler.INSTANCE,
            bytesReference, XContentType.JSON
        )
    }

    private fun createMessageWithHost(host: String): LegacyBaseMessage {
        return LegacyCustomWebhookMessage.Builder("abc")
            .withHost(host)
            .withPath("incomingwebhooks/383c0e2b-d028-44f4-8d38-696754bc4574")
            .withMessage("{\"Content\":\"Message test\"}")
            .withMethod("POST")
            .withQueryParams(HashMap<String, String>()).build()
    }

    private fun createMessageWithURl(url: String): LegacyBaseMessage {
        return LegacyCustomWebhookMessage.Builder("abc")
            .withUrl(url)
            .withPath("incomingwebhooks/383c0e2b-d028-44f4-8d38-696754bc4574")
            .withMessage("{\"Content\":\"Message test\"}")
            .withMethod("POST")
            .withQueryParams(HashMap<String, String>()).build()
    }
}
