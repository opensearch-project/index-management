/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import com.carrotsearch.randomizedtesting.RandomizedTest.sleep
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.index.Index
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.action.ReplicaCountAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.TransportExplainAction.Companion.METADATA_CORRUPT_WARNING
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.TransportExplainAction.Companion.METADATA_MOVING_WARNING
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale
import kotlin.collections.HashMap

class MetadataRegressionIT : IndexStateManagementIntegTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    @Before
    fun startMetadataService() {
        // metadata service could be stopped before following tests start run
        // this will enable metadata service again
        updateClusterSetting(ManagedIndexSettings.METADATA_SERVICE_STATUS.key, "-1")
        updateClusterSetting(ManagedIndexSettings.METADATA_SERVICE_STATUS.key, "0")
    }

    @After
    fun cleanClusterSetting() {
        // need to clean up otherwise will throw error
        updateClusterSetting(ManagedIndexSettings.METADATA_SERVICE_STATUS.key, null, false)
        updateClusterSetting(ManagedIndexSettings.TEMPLATE_MIGRATION_CONTROL.key, null, false)
        updateIndexStateManagementJitterSetting(null)
    }

    fun `test move metadata service`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = ReplicaCountAction(10, 0)
        val states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName)

        // create a job
        addPolicyToIndex(indexName, policyID)

        // put some metadata into cluster state
        var indexMetadata = getIndexMetadata(indexName)
        metadataToClusterState = metadataToClusterState.copy(
            index = indexName,
            indexUuid = indexMetadata.indexUUID,
            policyID = policyID
        )
        val request = UpdateManagedIndexMetaDataRequest(
            indicesToAddManagedIndexMetaDataTo = listOf(
                Pair(Index(metadataToClusterState.index, metadataToClusterState.indexUuid), metadataToClusterState)
            )
        )
        val response: AcknowledgedResponse = client().execute(
            UpdateManagedIndexMetaDataAction.INSTANCE, request
        ).get()
        logger.info(response.isAcknowledged)
        indexMetadata = getIndexMetadata(indexName)
        logger.info("check if metadata is saved in cluster state: ${indexMetadata.getCustomData("managed_index_metadata")}")

        waitFor {
            assertEquals(
                METADATA_MOVING_WARNING,
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        waitFor(Instant.ofEpochSecond(120)) {
            assertEquals(
                "Happy moving",
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
            assertEquals(null, getIndexMetadata(indexName).getCustomData("managed_index_metadata"))
        }

        logger.info("metadata has moved")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds, since there is metadata and policy with the index there is no initialization
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        waitFor {
            assertEquals(
                "Index did not set number_of_replicas to ${actionConfig.numOfReplicas}",
                actionConfig.numOfReplicas,
                getNumberOfReplicasSetting(indexName)
            )
        }
    }

    fun `test job can continue run from cluster state metadata`() {
        /**
         *  new version of ISM plugin can handle metadata in cluster state
         *  when job already started
         *
         *  create index, add policy to it
         *  manually add policy field to managed-index so runner won't do initialisation itself
         *  add metadata into cluster state
         *  then check if we can continue run from this added metadata
         */

        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"
        val actionConfig = ReplicaCountAction(10, 0)
        val states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName)
        addPolicyToIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // manually add policy field into managed-index
        updateManagedIndexConfigPolicy(managedIndexConfig, policy)
        logger.info("managed-index: ${getExistingManagedIndexConfig(indexName)}")

        // manually save metadata into cluster state
        var indexMetadata = getIndexMetadata(indexName)
        metadataToClusterState = metadataToClusterState.copy(
            index = indexName,
            indexUuid = indexMetadata.indexUUID,
            policyID = policyID
        )
        val request = UpdateManagedIndexMetaDataRequest(
            indicesToAddManagedIndexMetaDataTo = listOf(
                Pair(Index(metadataToClusterState.index, metadataToClusterState.indexUuid), metadataToClusterState)
            )
        )
        val response: AcknowledgedResponse = client().execute(
            UpdateManagedIndexMetaDataAction.INSTANCE, request
        ).get()

        logger.info(response.isAcknowledged)
        indexMetadata = getIndexMetadata(indexName)
        logger.info("check if metadata is saved in cluster state: ${indexMetadata.getCustomData("managed_index_metadata")}")

        waitFor {
            assertEquals(
                METADATA_MOVING_WARNING,
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        waitFor(Instant.ofEpochSecond(120)) {
            assertEquals(
                "Happy moving",
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
            assertEquals(null, getIndexMetadata(indexName).getCustomData("managed_index_metadata"))
        }

        logger.info("metadata has moved")

        // start the job run
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                "Index did not set number_of_replicas to ${actionConfig.numOfReplicas}",
                actionConfig.numOfReplicas,
                getNumberOfReplicasSetting(indexName)
            )
        }
    }

    fun `test clean corrupt metadata`() {
        val indexName = "${testIndexName}_index_3"
        val policyID = "${testIndexName}_testPolicyName_3"
        val action = ReplicaCountAction(10, 0)
        val states = listOf(State(name = "ReplicaCountState", actions = listOf(action), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName)

        // create a job
        addPolicyToIndex(indexName, policyID)

        // put some metadata into cluster state
        val indexMetadata = getIndexMetadata(indexName)
        metadataToClusterState = metadataToClusterState.copy(
            index = indexName,
            indexUuid = "randomindexuuid",
            policyID = policyID
        )
        val request = UpdateManagedIndexMetaDataRequest(
            indicesToAddManagedIndexMetaDataTo = listOf(
                Pair(Index(indexName, indexMetadata.indexUUID), metadataToClusterState)
            )
        )
        client().execute(UpdateManagedIndexMetaDataAction.INSTANCE, request).get()
        logger.info("check if metadata is saved in cluster state: ${getIndexMetadata(indexName).getCustomData("managed_index_metadata")}")

        waitFor {
            assertEquals(
                METADATA_CORRUPT_WARNING,
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        waitFor(Instant.ofEpochSecond(120)) {
            assertEquals(null, getExplainManagedIndexMetaData(indexName).info?.get("message"))
            assertEquals(null, getIndexMetadata(indexName).getCustomData("managed_index_metadata"))
        }

        logger.info("corrupt metadata has been cleaned")
    }

    fun `test new node skip execution when old node exist in cluster`() {
        Assume.assumeTrue(isMixedNodeRegressionTest)

        /**
         * mixedCluster-0 is new node, mixedCluster-1 is old node
         *
         * set config index to only have one shard on new node
         * so old node cannot run job because it has no shard
         * new node also cannot run job because there is an old node
         * here we check no job can be run
         *
         * then reroute shard to old node and this old node can run job
         */

        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = ReplicaCountAction(10, 0)
        val states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName)

        val settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0")
            .build()
        updateIndexSettings(INDEX_MANAGEMENT_INDEX, settings)

        // check config index shard position
        val shardsResponse = catIndexShard(INDEX_MANAGEMENT_INDEX)
        logger.info("check config index shard: $shardsResponse")
        val shardNode = (shardsResponse[0] as HashMap<*, *>)["node"]

        sleep(3000) // wait some time for cluster to be stable

        // move shard on node1 to node0 if exist
        if (shardNode == "mixedCluster-1") rerouteShard(INDEX_MANAGEMENT_INDEX, "mixedCluster-1", "mixedCluster-0")

        addPolicyToIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // check no job has been run
        wait { assertEquals(null, getExistingManagedIndexConfig(indexName).policy) }

        // reroute shard to node1
        rerouteShard(INDEX_MANAGEMENT_INDEX, "mixedCluster-0", "mixedCluster-1")

        val shardsResponse2 = catIndexShard(INDEX_MANAGEMENT_INDEX)
        logger.info("check config index shard: $shardsResponse2")

        // job can be ran now
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
    }
}
