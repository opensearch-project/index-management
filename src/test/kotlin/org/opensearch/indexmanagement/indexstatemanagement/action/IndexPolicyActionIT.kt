/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.junit.Assert
import org.opensearch.client.ResponseException
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.toJsonString
import org.opensearch.indexmanagement.makeRequest
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class IndexPolicyActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test allocation aware replica count`() {
        val policyID = "${testIndexName}_testPolicyName_replica"
        var actionConfig = ReplicaCountAction(3, 0)
        var states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        updateClusterSetting(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.key, "true")
        updateClusterSetting(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.key, "zone")

        // creates a dummy policy , so that ISM index gets initialized
        var policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        client().makeRequest(
            "PUT",
            "${IndexManagementPlugin.POLICY_BASE_URI}/init-index",
            emptyMap(),
            StringEntity(policy.toJsonString(), ContentType.APPLICATION_JSON)
        )

        updateClusterSetting(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.key + "zone.values", "a, b")

        // Valid replica count, shouldn't throw exception
        client().makeRequest(
            "PUT",
            "${IndexManagementPlugin.POLICY_BASE_URI}/$policyID",
            emptyMap(),
            StringEntity(policy.toJsonString(), ContentType.APPLICATION_JSON)
        )

        actionConfig = ReplicaCountAction(4, 0)
        states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        Assert.assertThrows(
            ResponseException::class.java
        ) {
            client().makeRequest(
                "PUT",
                "${IndexManagementPlugin.POLICY_BASE_URI}/$policyID",
                emptyMap(),
                StringEntity(policy.toJsonString(), ContentType.APPLICATION_JSON)
            )
        }

        // clean up cluster settings
        updateClusterSetting(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.key, "true")
        updateClusterSetting(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.key, "")
        updateClusterSetting(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.key + "zone", "")
    }
}
