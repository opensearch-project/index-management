/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.mockito.Mockito.doAnswer
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.indexmanagement.indexstatemanagement.action.AliasAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.step.alias.AttemptAliasActionsStep
import org.opensearch.indexmanagement.indexstatemanagement.toJsonString
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import java.time.Instant

class AttemptAliasActionsStepTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = mock()
    private val indexName = "test-index"

    fun `test alias step applies the actions on the managed index`() {
        val requests = mutableListOf<IndicesAliasesRequest>()
        val aliasAction = AliasAction(listOf(IndicesAliasesRequest.AliasActions.add().aliases("my-alias")), 0)

        val step = execute(aliasAction, requests)

        assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, step.getUpdatedManagedIndexMetadata(metadata()).stepMetaData?.stepStatus)
        assertEquals(1, requests.size)
        val sent = requests[0].aliasActions
        assertEquals(1, sent.size)
        assertArrayEquals(arrayOf(indexName), sent[0].indices())
        assertArrayEquals(arrayOf("my-alias"), sent[0].aliases())
        assertEquals(IndicesAliasesRequest.AliasActions.Type.ADD, sent[0].actionType())
    }

    fun `test alias step keeps the other settings of the actions`() {
        val requests = mutableListOf<IndicesAliasesRequest>()
        val aliasAction = AliasAction(
            listOf(
                IndicesAliasesRequest.AliasActions.add().aliases("my-alias").routing("r1").writeIndex(true).isHidden(true),
                IndicesAliasesRequest.AliasActions.remove().aliases("old-alias"),
            ),
            0,
        )

        execute(aliasAction, requests)

        val sent = requests.single().aliasActions
        assertEquals(2, sent.size)
        assertEquals("r1", sent[0].indexRouting())
        assertEquals(true, sent[0].writeIndex())
        assertEquals(true, sent[0].isHidden())
        assertEquals(IndicesAliasesRequest.AliasActions.Type.REMOVE, sent[1].actionType())
        assertArrayEquals(arrayOf("old-alias"), sent[1].aliases())
        sent.forEach { assertArrayEquals(arrayOf(indexName), it.indices()) }
    }

    fun `test alias step does not change the actions of the policy`() {
        val requests = mutableListOf<IndicesAliasesRequest>()
        val aliasAction = AliasAction(listOf(IndicesAliasesRequest.AliasActions.add().aliases("my-alias")), 0)

        execute(aliasAction, requests)

        // The same step can run again, for example on the next job run, and must still send the managed index
        execute(aliasAction, requests)

        assertTrue("The actions of the policy must not get an index", aliasAction.actions.all { it.indices().isNullOrEmpty() })
        assertEquals(2, requests.size)
        requests.forEach { assertArrayEquals(arrayOf(indexName), it.aliasActions.single().indices()) }
    }

    // Regression test: the managed index config, including its policy, is written back to the config index after
    // a step ran (for example when the job interval changes) and has to be parsable afterwards.
    fun `test managed index config can be parsed after the alias step ran`() {
        val aliasAction = AliasAction(listOf(IndicesAliasesRequest.AliasActions.add().aliases("my-alias")), 0)
        val policy = randomPolicy(states = listOf(randomState(actions = listOf(aliasAction))))
        val managedIndexConfig = randomManagedIndexConfig(index = indexName, policy = policy)
        val policyAliasAction = managedIndexConfig.policy.states[0].actions[0] as AliasAction

        execute(policyAliasAction, mutableListOf())

        val parsed = parserWithType(managedIndexConfig.toJsonString()).parseWithType(parse = ManagedIndexConfig.Companion::parse)
        val parsedAliasAction = parsed.policy.states[0].actions[0] as AliasAction
        assertTrue(parsedAliasAction.actions.all { it.indices().isNullOrEmpty() })
        assertArrayEquals(arrayOf("my-alias"), parsedAliasAction.actions.single().aliases())
    }

    private fun execute(aliasAction: AliasAction, requests: MutableList<IndicesAliasesRequest>): Step {
        val client = getClient(getAdminClient(getIndicesAdminClient(requests)))
        val step = AttemptAliasActionsStep(aliasAction)
        runBlocking {
            val context = StepContext(metadata(), clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
        }
        // A step that failed before doing its work must not let a test pass
        val status = step.getUpdatedManagedIndexMetadata(metadata()).stepMetaData?.stepStatus
        assertEquals("Step status is not COMPLETED: ${step.getUpdatedManagedIndexMetadata(metadata()).info}", Step.StepStatus.COMPLETED, status)
        return step
    }

    private fun metadata() = ManagedIndexMetaData(
        indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
        ActionMetaData("alias", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null,
    )

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }

    private fun getIndicesAdminClient(requests: MutableList<IndicesAliasesRequest>): IndicesAdminClient = mock {
        doAnswer { invocationOnMock ->
            requests.add(invocationOnMock.getArgument(0))
            invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1).onResponse(AcknowledgedResponse(true))
        }.whenever(this.mock).aliases(any(), any())
    }

    private fun parserWithType(xc: String): XContentParser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
}
