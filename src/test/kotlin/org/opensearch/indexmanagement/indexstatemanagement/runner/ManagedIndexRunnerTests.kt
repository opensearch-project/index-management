/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.runner

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.update.UpdateRequest
import org.opensearch.action.update.UpdateResponse
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.routing.Preference
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesArray
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.env.Environment
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementHistory
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.SkipExecution
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.randomManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.toJsonString
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.script.ScriptService
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.time.Instant

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ManagedIndexRunnerTests : OpenSearchTestCase() {
    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var environment: Environment
    private lateinit var indexStateManagementHistory: IndexStateManagementHistory
    private lateinit var skipFlag: SkipExecution
    private lateinit var imIndices: IndexManagementIndices
    private lateinit var indexMetadataProvider: IndexMetadataProvider
    private lateinit var runner: ManagedIndexRunner

    private lateinit var settings: Settings
    private lateinit var discoveryNode: DiscoveryNode
    private lateinit var threadPool: ThreadPool

    @Before
    @Throws(Exception::class)
    fun setup() {
        clusterService = Mockito.mock(ClusterService::class.java)
        xContentRegistry = NamedXContentRegistry.EMPTY
        scriptService = Mockito.mock(ScriptService::class.java)
        environment = Mockito.mock(Environment::class.java)
        indexStateManagementHistory = Mockito.mock(IndexStateManagementHistory::class.java)
        skipFlag = Mockito.mock(SkipExecution::class.java)
        imIndices = mock { onBlocking { attemptUpdateConfigIndexMapping() } doReturn true }

        threadPool = Mockito.mock(ThreadPool::class.java)
        settings = Settings.builder().build()
        discoveryNode = DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT)
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(ManagedIndexSettings.SWEEP_PERIOD)
        settingSet.add(ManagedIndexSettings.JITTER)
        settingSet.add(ManagedIndexSettings.JOB_INTERVAL)
        settingSet.add(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED)
        settingSet.add(ManagedIndexSettings.ACTION_VALIDATION_ENABLED)
        settingSet.add(ManagedIndexSettings.ALLOW_LIST)
        settingSet.add(ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER)
        settingSet.add(ManagedIndexSettings.JOB_DOCUMENT_CHECK_ENABLED)
        settingSet.add(ManagedIndexSettings.POLICY_VERSION_RECONCILIATION_ENABLED)
        settingSet.add(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN)
        val clusterSettings = ClusterSettings(settings, settingSet)
        val originClusterService: ClusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings)
        clusterService = Mockito.spy(originClusterService)

        Mockito.`when`(environment.settings()).thenReturn(settings)
        client = Mockito.mock(Client::class.java)
        // no index metadata services: the provider answers with an empty map (index creation date unknown)
        indexMetadataProvider = IndexMetadataProvider(settings, client, clusterService, mutableMapOf())

        runner =
            ManagedIndexRunner
                .registerClusterService(clusterService)
                .registerClient(client)
                .registerNamedXContentRegistry(xContentRegistry)
                .registerScriptService(scriptService)
                .registerSettings(environment.settings())
                .registerConsumers()
                .registerHistoryIndex(indexStateManagementHistory)
                .registerSkipFlag(skipFlag)
                .registerIMIndex(imIndices)
                .registerIndexMetadataProvider(indexMetadataProvider)
    }

    fun `test job document check allows initialization when the job document exists`() {
        val managedIndexConfig = randomManagedIndexConfig()
        val requests = mockJobDocumentGet(exists = true)

        val exists = runBlocking { runner.jobDocumentExists(managedIndexConfig) }

        assertTrue("run should proceed when the job document exists", exists)
        assertEquals("exactly one GET expected", 1, requests.size)
    }

    fun `test job document check uses a realtime primary GET without source`() {
        val managedIndexConfig = randomManagedIndexConfig()
        val requests = mockJobDocumentGet(exists = true)

        runBlocking { runner.jobDocumentExists(managedIndexConfig) }

        val request = requests.single()
        assertEquals(INDEX_MANAGEMENT_INDEX, request.index())
        assertEquals(managedIndexConfig.indexUuid, request.id())
        assertEquals("job doc and metadata doc share the routing", managedIndexConfig.indexUuid, request.routing())
        assertTrue("lookup must be realtime to see unrefreshed writes", request.realtime())
        assertEquals("lookup must be served by the primary copy", Preference.PRIMARY.type(), request.preference())
        assertFalse("existence check must not fetch the source", request.fetchSourceContext().fetchSource())
    }

    fun `test job document check skips initialization when the job document is gone`() {
        val managedIndexConfig = randomManagedIndexConfig()
        mockJobDocumentGet(exists = false)

        val exists = runBlocking { runner.jobDocumentExists(managedIndexConfig) }

        assertFalse("run must be skipped when the job document no longer exists", exists)
    }

    fun `test job document check aborts the run when the lookup fails`() {
        val managedIndexConfig = randomManagedIndexConfig()
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<GetResponse>>(1)
            listener.onFailure(RuntimeException("config index unavailable"))
        }.whenever(client).get(any(), any())

        val exists = runBlocking { runner.jobDocumentExists(managedIndexConfig) }

        assertFalse("run must be aborted when the job document cannot be verified", exists)
    }

    fun `test job document check is bypassed when disabled`() {
        val managedIndexConfig = randomManagedIndexConfig()
        mockJobDocumentGet(exists = false)
        applyJobDocumentCheckSetting(false)
        try {
            val exists = runBlocking { runner.jobDocumentExists(managedIndexConfig) }

            assertTrue("disabled check must fall back to the previous behaviour", exists)
            verify(client, never()).get(any(), any())
        } finally {
            applyJobDocumentCheckSetting(true)
        }
    }

    fun `test job document check follows dynamic setting updates`() {
        val managedIndexConfig = randomManagedIndexConfig()
        mockJobDocumentGet(exists = false)

        applyJobDocumentCheckSetting(false)
        assertTrue(runBlocking { runner.jobDocumentExists(managedIndexConfig) })
        verify(client, never()).get(any(), any())

        applyJobDocumentCheckSetting(true)
        assertFalse(runBlocking { runner.jobDocumentExists(managedIndexConfig) })
        verify(client, times(1)).get(any(), any())
    }

    private fun applyJobDocumentCheckSetting(enabled: Boolean) {
        clusterService.clusterSettings.applySettings(
            Settings.builder().put(ManagedIndexSettings.JOB_DOCUMENT_CHECK_ENABLED.key, enabled).build(),
        )
    }

    /** Answers every client.get with a response whose isExists is [exists] and records the requests it received. */
    private fun mockJobDocumentGet(exists: Boolean): MutableList<GetRequest> {
        val requests = mutableListOf<GetRequest>()
        val getResponse: GetResponse = mock()
        whenever(getResponse.isExists).doReturn(exists)
        doAnswer { invocation ->
            requests.add(invocation.getArgument<GetRequest>(0))
            val listener = invocation.getArgument<ActionListener<GetResponse>>(1)
            listener.onResponse(getResponse)
        }.whenever(client).get(any(), any())
        return requests
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Policy version reconciliation: the metadata's policy version differs from the job the scheduler handed over
    // ---------------------------------------------------------------------------------------------------------------

    fun `test policy version reconciliation continues with the job document when it agrees with the metadata`() {
        val indexUuid = randomAlphaOfLength(20)
        val scheduledPolicy = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val currentPolicy = scheduledPolicy.copy(seqNo = 6)
        val scheduled = testConfig(indexUuid, scheduledPolicy)
        val metadata = testMetadata(indexUuid, currentPolicy)
        val configIndex = ConfigIndexMock(indexUuid to Document(testConfig(indexUuid, currentPolicy).toJsonString(), seqNo = 11, primaryTerm = 2))

        val continueWith = runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) }

        assertNotNull("run should continue when the job document agrees with the metadata", continueWith)
        assertEquals("run must continue with the job document's policy version", 6L, continueWith!!.policySeqNo)
        assertEquals("the job document's seqNo must be carried for later writes", 11L, continueWith.seqNo)
        assertEquals("the job document's primaryTerm must be carried for later writes", 2L, continueWith.primaryTerm)
        assertEquals("exactly one lookup of the job document expected", listOf(indexUuid), configIndex.getRequests.map { it.id() })
        assertTrue("nothing may be written when only the scheduler copy is stale", configIndex.indexRequests.isEmpty())
        assertTrue("the job must not be disabled", configIndex.updateRequests.isEmpty())
    }

    fun `test policy version reconciliation aborts the run when the job document lookup fails`() {
        val indexUuid = randomAlphaOfLength(20)
        val scheduledPolicy = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val scheduled = testConfig(indexUuid, scheduledPolicy)
        val metadata = testMetadata(indexUuid, scheduledPolicy.copy(seqNo = 6))
        val configIndex = ConfigIndexMock()
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<GetResponse>>(1)
            listener.onFailure(RuntimeException("config index unavailable"))
        }.whenever(client).get(any(), any())

        val continueWith = runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) }

        assertNull("run must be aborted when the job document cannot be read", continueWith)
        assertTrue("nothing may be written when the lookup failed", configIndex.indexRequests.isEmpty())
        assertTrue("the job must not be disabled", configIndex.updateRequests.isEmpty())
    }

    fun `test policy version reconciliation skips the run when the job document is gone`() {
        val indexUuid = randomAlphaOfLength(20)
        val scheduledPolicy = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val scheduled = testConfig(indexUuid, scheduledPolicy)
        val metadata = testMetadata(indexUuid, scheduledPolicy.copy(seqNo = 6))
        val configIndex = ConfigIndexMock() // no job document

        val continueWith = runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) }

        assertNull("an orphaned in-memory job must not run", continueWith)
        assertTrue("nothing may be written for an orphaned job", configIndex.indexRequests.isEmpty())
        assertTrue("the job must not be disabled", configIndex.updateRequests.isEmpty())
    }

    fun `test policy version reconciliation replays an interrupted policy change`() {
        val indexUuid = randomAlphaOfLength(20)
        val policyA = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val policyB = testPolicy("policy-b", seqNo = 11, primaryTerm = 2)
        val changeToB = ChangePolicy(policyB.id, null, emptyList(), isSafe = true)
        // initChangePolicy already wrote the metadata for policy B, then failed to write the job document
        val scheduled = testConfig(indexUuid, policyA, changePolicy = changeToB)
        // the first write kept the action metadata of the action that was executing when the change started
        val actionAtChange = ActionMetaData("open", 1234L, 0, false, 0, null, null)
        val metadata = testMetadata(indexUuid, policyB).copy(actionMetaData = actionAtChange)
        val configIndex =
            ConfigIndexMock(
                indexUuid to Document(scheduled.toJsonString(), seqNo = 21, primaryTerm = 2),
                policyB.id to Document(policyB.toJsonString(), seqNo = policyB.seqNo, primaryTerm = policyB.primaryTerm),
            )

        val continueWith = runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) }

        assertNull("the run ends after replaying the policy change", continueWith)
        assertEquals("metadata then job document must be written", listOf(managedIndexMetadataID(indexUuid), indexUuid), configIndex.indexRequests.map { it.id() })
        val writtenMetadata = configIndex.metadataSource(0)
        assertEquals(policyB.id, writtenMetadata[ManagedIndexMetaData.POLICY_ID])
        assertEquals(policyB.seqNo.toInt(), writtenMetadata[ManagedIndexMetaData.POLICY_SEQ_NO])
        assertEquals("Attempting to change policy to ${policyB.id}", (writtenMetadata[ManagedIndexMetaData.INFO] as Map<*, *>)["message"])
        val writtenAction = writtenMetadata[ActionMetaData.ACTION] as Map<*, *>
        assertEquals("the replay must keep the action metadata of the first write", actionAtChange.name, writtenAction[ManagedIndexMetaData.NAME])
        assertEquals("the replay must not restart the action", actionAtChange.startTime?.toInt(), writtenAction[ManagedIndexMetaData.START_TIME])
        val writtenConfig = configIndex.configSource(1)
        assertEquals(policyB.id, writtenConfig[ManagedIndexConfig.POLICY_ID_FIELD])
        assertEquals(policyB.seqNo.toInt(), writtenConfig[ManagedIndexConfig.POLICY_SEQ_NO_FIELD])
        assertNull("the change_policy marker must be cleared", writtenConfig[ManagedIndexConfig.CHANGE_POLICY_FIELD])
        assertEquals("the job document write must be guarded by the document's seqNo", 21L, configIndex.indexRequests[1].ifSeqNo())
        assertTrue("the job must not be disabled", configIndex.updateRequests.isEmpty())
    }

    fun `test policy version reconciliation fails the managed index when the metadata was written under a foreign policy version`() {
        val indexUuid = randomAlphaOfLength(20)
        val policyA = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val foreignPolicy = testPolicy("policy-old", seqNo = 2, primaryTerm = 1, state = "warm")
        val scheduled = testConfig(indexUuid, policyA)
        val metadata = testMetadata(indexUuid, foreignPolicy, seqNo = 7, primaryTerm = 1)
        val configIndex = ConfigIndexMock(indexUuid to Document(scheduled.toJsonString(), seqNo = 21, primaryTerm = 2))

        val continueWith = runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) }

        assertNull("the run ends after failing the managed index", continueWith)
        assertTrue("the job document must be consulted first", configIndex.getRequests.any { it.id() == indexUuid })
        assertVersionConflictFailure(configIndex, indexUuid)
    }

    fun `test policy version reconciliation does not replay a change whose metadata carries another version of the target`() {
        val indexUuid = randomAlphaOfLength(20)
        val policyA = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val policyB = testPolicy("policy-b", seqNo = 11, primaryTerm = 2)
        val changeToB = ChangePolicy(policyB.id, null, emptyList(), isSafe = false)
        val scheduled = testConfig(indexUuid, policyA, changePolicy = changeToB)
        // the metadata names the change target, but was stamped with a version of it the first write cannot have written
        val metadata = testMetadata(indexUuid, policyB.copy(seqNo = 9))
        val configIndex =
            ConfigIndexMock(
                indexUuid to Document(scheduled.toJsonString(), seqNo = 21, primaryTerm = 2),
                policyB.id to Document(policyB.toJsonString(), seqNo = policyB.seqNo, primaryTerm = policyB.primaryTerm),
            )

        assertNull(runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) })

        assertVersionConflictFailure(configIndex, indexUuid)
    }

    fun `test policy version reconciliation does not replay a change whose metadata is not its first write`() {
        val indexUuid = randomAlphaOfLength(20)
        val policyA = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val policyB = testPolicy("policy-b", seqNo = 11, primaryTerm = 2)
        val changeToB = ChangePolicy(policyB.id, null, emptyList(), isSafe = false)
        val scheduled = testConfig(indexUuid, policyA, changePolicy = changeToB)
        // correctly stamped, but a step is recorded: the first write of a policy change always clears the step metadata
        val metadata = testMetadata(indexUuid, policyB).copy(stepMetaData = StepMetaData("attempt_open", 1234L, Step.StepStatus.COMPLETED))
        val configIndex =
            ConfigIndexMock(
                indexUuid to Document(scheduled.toJsonString(), seqNo = 21, primaryTerm = 2),
                policyB.id to Document(policyB.toJsonString(), seqNo = policyB.seqNo, primaryTerm = policyB.primaryTerm),
            )

        assertNull(runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) })

        assertVersionConflictFailure(configIndex, indexUuid)
    }

    fun `test policy version reconciliation fails the managed index when the change target policy no longer exists`() {
        val indexUuid = randomAlphaOfLength(20)
        val policyA = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val policyB = testPolicy("policy-b", seqNo = 11, primaryTerm = 2)
        val changeToB = ChangePolicy(policyB.id, null, emptyList(), isSafe = false)
        val scheduled = testConfig(indexUuid, policyA, changePolicy = changeToB)
        val metadata = testMetadata(indexUuid, policyB)
        // policy B was deleted after the interrupted first write
        val configIndex = ConfigIndexMock(indexUuid to Document(scheduled.toJsonString(), seqNo = 21, primaryTerm = 2))

        assertNull(runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) })

        assertTrue("the target policy must be looked up", configIndex.getRequests.any { it.id() == policyB.id })
        assertVersionConflictFailure(configIndex, indexUuid)
    }

    fun `test policy version reconciliation fails the managed index after the attempts are exhausted`() {
        val indexUuid = randomAlphaOfLength(20)
        val policyA = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val policyB = testPolicy("policy-b", seqNo = 11, primaryTerm = 2)
        val scheduled = testConfig(indexUuid, policyA, changePolicy = ChangePolicy(policyB.id, null, emptyList(), isSafe = false))
        val metadata = testMetadata(indexUuid, policyB)
        val configIndex =
            ConfigIndexMock(
                indexUuid to Document(scheduled.toJsonString(), seqNo = 21, primaryTerm = 2),
                policyB.id to Document(policyB.toJsonString(), seqNo = policyB.seqNo, primaryTerm = policyB.primaryTerm),
            )
        // the second write of the replayed change, the job document, keeps losing its version check
        configIndex.failIndexRequestWhen = { it.id() == indexUuid }

        repeat(3) { attempt ->
            assertNull(runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) })
            assertEquals(
                "attempt ${attempt + 1} must replay the change: metadata then job document",
                (attempt + 1) * 2, configIndex.indexRequests.size,
            )
            assertEquals(indexUuid, configIndex.indexRequests.last().id())
            assertTrue("the job must not be disabled while attempts remain", configIndex.updateRequests.isEmpty())
        }

        assertNull(runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) })

        assertEquals("the fourth run falls back to failing the managed index", 7, configIndex.indexRequests.size)
        assertEquals(managedIndexMetadataID(indexUuid), configIndex.indexRequests.last().id())
        val failure = configIndex.metadataSource(6)
        assertEquals(true, (failure[PolicyRetryInfoMetaData.RETRY_INFO] as Map<*, *>)["failed"])
        assertEquals(
            "There is a version conflict between your previous execution and your managed index",
            (failure[ManagedIndexMetaData.INFO] as Map<*, *>)["message"],
        )
        assertEquals("the job must be disabled once", listOf(indexUuid), configIndex.updateRequests.map { it.id() })
    }

    fun `test policy version reconciliation attempts reset once the documents agree`() {
        val indexUuid = randomAlphaOfLength(20)
        val policyA = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val policyB = testPolicy("policy-b", seqNo = 11, primaryTerm = 2)
        val scheduled = testConfig(indexUuid, policyA, changePolicy = ChangePolicy(policyB.id, null, emptyList(), isSafe = false))
        val interrupted = testMetadata(indexUuid, policyB)
        val configIndex =
            ConfigIndexMock(
                indexUuid to Document(scheduled.toJsonString(), seqNo = 21, primaryTerm = 2),
                policyB.id to Document(policyB.toJsonString(), seqNo = policyB.seqNo, primaryTerm = policyB.primaryTerm),
            )
        configIndex.failIndexRequestWhen = { it.id() == indexUuid }

        repeat(2) { assertNull(runBlocking { runner.reconcilePolicyVersion(scheduled, interrupted) }) }
        // the documents agree on the next run (metadata now carries the job document's version): attempts are reset
        assertNotNull(runBlocking { runner.reconcilePolicyVersion(scheduled.copy(policySeqNo = 4), testMetadata(indexUuid, policyA)) })
        // three more non-converging replays are needed before the managed index is failed
        repeat(3) { assertNull(runBlocking { runner.reconcilePolicyVersion(scheduled, interrupted) }) }

        assertEquals("five replays of two writes each", 10, configIndex.indexRequests.size)
        assertTrue(
            "all writes so far are replays of the change",
            configIndex.indexRequests.all { it.id() == indexUuid || it.sourceContains("Attempting to change policy to ${policyB.id}") },
        )
        assertTrue("the job must not be disabled", configIndex.updateRequests.isEmpty())
    }

    fun `test policy version reconciliation fails the managed index immediately when disabled`() {
        val indexUuid = randomAlphaOfLength(20)
        val policyA = testPolicy("policy-a", seqNo = 5, primaryTerm = 1)
        val scheduled = testConfig(indexUuid, policyA)
        val metadata = testMetadata(indexUuid, policyA.copy(seqNo = 6))
        val configIndex = ConfigIndexMock(indexUuid to Document(testConfig(indexUuid, policyA.copy(seqNo = 6)).toJsonString(), seqNo = 11, primaryTerm = 2))
        applyPolicyVersionReconciliationSetting(false)
        try {
            val continueWith = runBlocking { runner.reconcilePolicyVersion(scheduled, metadata) }

            assertNull("the run ends after failing the managed index", continueWith)
            assertTrue("the job document must not be consulted when disabled", configIndex.getRequests.isEmpty())
            val failure = configIndex.metadataSource(0)
            assertEquals(true, (failure[PolicyRetryInfoMetaData.RETRY_INFO] as Map<*, *>)["failed"])
            assertEquals(
                "There is a version conflict between your previous execution and your managed index",
                (failure[ManagedIndexMetaData.INFO] as Map<*, *>)["message"],
            )
            assertEquals("the job must be disabled", listOf(indexUuid), configIndex.updateRequests.map { it.id() })
        } finally {
            applyPolicyVersionReconciliationSetting(true)
        }
    }

    /** The pre-reconciliation outcome: the metadata is failed with the version conflict message and the job is disabled. */
    private fun assertVersionConflictFailure(configIndex: ConfigIndexMock, indexUuid: String) {
        assertEquals("exactly the failed metadata must be written", listOf(managedIndexMetadataID(indexUuid)), configIndex.indexRequests.map { it.id() })
        val failure = configIndex.metadataSource(0)
        assertEquals(true, (failure[PolicyRetryInfoMetaData.RETRY_INFO] as Map<*, *>)["failed"])
        assertEquals(
            "There is a version conflict between your previous execution and your managed index",
            (failure[ManagedIndexMetaData.INFO] as Map<*, *>)["message"],
        )
        assertEquals("the job must be disabled", listOf(indexUuid), configIndex.updateRequests.map { it.id() })
    }

    private fun applyPolicyVersionReconciliationSetting(enabled: Boolean) {
        clusterService.clusterSettings.applySettings(
            Settings.builder().put(ManagedIndexSettings.POLICY_VERSION_RECONCILIATION_ENABLED.key, enabled).build(),
        )
    }

    private fun testPolicy(id: String, seqNo: Long, primaryTerm: Long, state: String = "hot"): Policy =
        randomPolicy(id = id, states = listOf(randomState(name = state))).copy(seqNo = seqNo, primaryTerm = primaryTerm)

    private fun testConfig(indexUuid: String, policy: Policy, changePolicy: ChangePolicy? = null): ManagedIndexConfig =
        randomManagedIndexConfig(index = "reconcile_index", uuid = indexUuid, enabled = true, policy = policy, changePolicy = changePolicy)

    private fun testMetadata(indexUuid: String, policy: Policy, seqNo: Long = 7, primaryTerm: Long = 1): ManagedIndexMetaData =
        ManagedIndexMetaData(
            index = "reconcile_index",
            indexUuid = indexUuid,
            policyID = policy.id,
            policySeqNo = policy.seqNo,
            policyPrimaryTerm = policy.primaryTerm,
            policyCompleted = false,
            rolledOver = false,
            indexCreationDate = null,
            transitionTo = null,
            stateMetaData = StateMetaData(policy.defaultState, Instant.now().toEpochMilli()),
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "running"),
            seqNo = seqNo,
            primaryTerm = primaryTerm,
        )

    private fun IndexRequest.sourceContains(text: String): Boolean = source().utf8ToString().contains(text)

    private data class Document(val source: String, val seqNo: Long, val primaryTerm: Long)

    /** A mocked config index: serves [documents] to GETs, records every write, and optionally rejects writes with a version conflict. */
    private inner class ConfigIndexMock(vararg documents: Pair<String, Document>) {
        val documents = mutableMapOf(*documents)
        val getRequests = mutableListOf<GetRequest>()
        val indexRequests = mutableListOf<IndexRequest>()
        val updateRequests = mutableListOf<UpdateRequest>()
        var failIndexRequestWhen: (IndexRequest) -> Boolean = { false }

        init {
            doAnswer { invocation ->
                val request = invocation.getArgument<GetRequest>(0)
                val listener = invocation.getArgument<ActionListener<GetResponse>>(1)
                getRequests.add(request)
                val document = this.documents[request.id()]
                val response: GetResponse = mock()
                whenever(response.id).doReturn(request.id())
                whenever(response.isExists).doReturn(document != null)
                whenever(response.isSourceEmpty).doReturn(document == null)
                if (document != null) {
                    whenever(response.seqNo).doReturn(document.seqNo)
                    whenever(response.primaryTerm).doReturn(document.primaryTerm)
                    whenever(response.sourceAsBytesRef).doReturn(BytesArray(document.source))
                }
                listener.onResponse(response)
            }.whenever(client).get(any(), any())
            doAnswer { invocation ->
                val request = invocation.getArgument<IndexRequest>(0)
                val listener = invocation.getArgument<ActionListener<IndexResponse>>(1)
                indexRequests.add(request)
                if (failIndexRequestWhen(request)) {
                    listener.onFailure(VersionConflictEngineException(ShardId(INDEX_MANAGEMENT_INDEX, "_na_", 0), request.id(), "stale write"))
                } else {
                    val response: IndexResponse = mock()
                    whenever(response.status()).doReturn(RestStatus.OK)
                    whenever(response.seqNo).doReturn(30L)
                    whenever(response.primaryTerm).doReturn(2L)
                    listener.onResponse(response)
                }
            }.whenever(client).index(any(), any())
            doAnswer { invocation ->
                val request = invocation.getArgument<UpdateRequest>(0)
                val listener = invocation.getArgument<ActionListener<UpdateResponse>>(1)
                updateRequests.add(request)
                val response: UpdateResponse = mock()
                whenever(response.status()).doReturn(RestStatus.OK)
                listener.onResponse(response)
            }.whenever(client).update(any(), any())
        }

        /** The managed_index_metadata object of the i-th write. */
        @Suppress("UNCHECKED_CAST")
        fun metadataSource(i: Int): Map<String, Any?> =
            XContentHelper.convertToMap(indexRequests[i].source(), false, XContentType.JSON).v2()[ManagedIndexMetaData.MANAGED_INDEX_METADATA_TYPE] as Map<String, Any?>

        /** The managed_index object of the i-th write. */
        @Suppress("UNCHECKED_CAST")
        fun configSource(i: Int): Map<String, Any?> =
            XContentHelper.convertToMap(indexRequests[i].source(), false, XContentType.JSON).v2()[ManagedIndexConfig.MANAGED_INDEX_TYPE] as Map<String, Any?>
    }
}
