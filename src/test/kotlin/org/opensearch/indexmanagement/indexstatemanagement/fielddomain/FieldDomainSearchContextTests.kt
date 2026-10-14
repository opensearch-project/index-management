/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.fielddomain

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.action.support.IndicesOptions
import org.opensearch.common.CheckedRunnable
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.identity.PluginSubject
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.Client
import java.security.Principal
import kotlin.test.assertFailsWith

class FieldDomainSearchContextTests : OpenSearchTestCase() {
    private val threadContext = ThreadContext(Settings.EMPTY)
    private val subject = object : PluginSubject {
        override fun getPrincipal(): Principal = Principal { PLUGIN }

        override fun <E : Exception> runAs(runnable: CheckedRunnable<E>) {
            threadContext.stashContext().use {
                threadContext.putTransient(IDENTITY, PLUGIN)
                runnable.run()
            }
        }
    }

    override fun setUp() {
        super.setUp()
        threadContext.putTransient(IDENTITY, POLICY)
    }

    fun `test non date searches use plugin identity and the exact managed index`() = runBlocking {
        val response = response()
        val source = SearchSourceBuilder().size(0)
            .query(QueryBuilders.existsQuery("tenant"))
            .aggregation(AggregationBuilders.terms("tenants").field("tenant"))
        val searchClient = client { request, listener ->
            assertArrayEquals(arrayOf(INDEX_NAME), request.indices())
            assertSame(source, request.source())
            assertEquals(IndicesOptions.strictSingleIndexNoExpandForbidClosed(), request.indicesOptions())
            assertEquals(false, request.allowPartialSearchResults())
            listener.onResponse(response)
        }

        val actual = FieldDomainSearchContext(stepContext(searchClient)) { subject }.search(source)

        assertSame(response, actual)
        assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
    }

    fun `test identity is resolved for each search and missing identity prevents dispatch`() = runBlocking {
        var assignedSubject: PluginSubject? = null
        val response = response()
        val searchClient = client { _, listener -> listener.onResponse(response) }
        val context = FieldDomainSearchContext(stepContext(searchClient)) { checkNotNull(assignedSubject) }

        assertFailsWith<IllegalStateException> { context.search(SearchSourceBuilder()) }
        verify(searchClient, never()).search(any(), any())

        assignedSubject = subject
        assertSame(response, context.search(SearchSourceBuilder()))
        assertSame(response, context.search(SearchSourceBuilder()))
        assignedSubject = null
        assertFailsWith<IllegalStateException> { context.search(SearchSourceBuilder()) }
        verify(searchClient, times(2)).search(any(), any())
        assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
    }

    fun `test missing thread context prevents dispatch`() = runBlocking {
        val searchClient = mock<Client>()
        val context = FieldDomainSearchContext(stepContext(searchClient, null)) { subject }

        assertFailsWith<IllegalStateException> { context.search(SearchSourceBuilder()) }

        verify(searchClient, never()).search(any(), any())
        assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
    }

    fun `test timed out responses are rejected`() {
        assertRejected(response(timedOut = true), "timed out")
    }

    fun `test early terminated responses are rejected`() {
        assertRejected(response(terminatedEarly = true), "terminated early")
    }

    fun `test shard failure details are rejected`() {
        assertRejected(
            response(failures = arrayOf(ShardSearchFailure(IllegalStateException("shard unavailable")))),
            "shard unavailable",
        )
    }

    fun `test failed shard count is rejected even without failure details`() {
        assertRejected(response(failedShards = 1), "shard failures")
    }

    fun `test failed search restores policy identity`() = runBlocking {
        val failure = IllegalStateException("search failed")
        val searchClient = client { _, listener -> listener.onFailure(failure) }
        val context = FieldDomainSearchContext(stepContext(searchClient)) { subject }

        val actual = assertFailsWith<IllegalStateException> { context.search(SearchSourceBuilder()) }

        assertEquals(failure.message, actual.message)
        assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
    }

    fun `test dispatch failure restores policy identity`() = runBlocking {
        val failure = IllegalStateException("dispatch failed")
        val searchClient = client { _, _ -> throw failure }
        val context = FieldDomainSearchContext(stepContext(searchClient)) { subject }

        val actual = assertFailsWith<IllegalStateException> { context.search(SearchSourceBuilder()) }

        assertSame(failure, actual)
        assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
    }

    private fun assertRejected(response: SearchResponse, message: String) = runBlocking {
        val searchClient = client { _, listener -> listener.onResponse(response) }
        val context = FieldDomainSearchContext(stepContext(searchClient)) { subject }

        val failure = assertFailsWith<IllegalStateException> { context.search(SearchSourceBuilder()) }

        assertTrue(failure.message!!.contains(INDEX_NAME))
        assertTrue(failure.message!!.contains(message))
        assertEquals(POLICY, threadContext.getTransient<String>(IDENTITY))
    }

    private fun client(dispatch: (SearchRequest, ActionListener<SearchResponse>) -> Unit): Client = mock {
        doAnswer { invocation ->
            assertEquals(PLUGIN, threadContext.getTransient<String>(IDENTITY))
            dispatch(invocation.getArgument(0), invocation.getArgument(1))
        }.whenever(this.mock).search(any(), any())
    }

    private fun response(
        timedOut: Boolean = false,
        terminatedEarly: Boolean? = null,
        failedShards: Int = 0,
        failures: Array<ShardSearchFailure> = ShardSearchFailure.EMPTY_ARRAY,
    ): SearchResponse = mock {
        on { isTimedOut } doReturn timedOut
        on { isTerminatedEarly } doReturn terminatedEarly
        on { getFailedShards() } doReturn failedShards
        on { shardFailures } doReturn failures
    }

    private fun stepContext(client: Client, threadContext: ThreadContext? = this.threadContext): StepContext = StepContext(
        ManagedIndexMetaData(
            INDEX_NAME, "test-uuid", "test-policy", null, null, null, null, null, null, null, null, null, null, null,
        ),
        mock(),
        client,
        threadContext,
        null,
        mock(),
        Settings.EMPTY,
        mock(),
    )

    companion object {
        private const val INDEX_NAME = "catalog-000001"
        private const val IDENTITY = "test_identity"
        private const val POLICY = "policy-user"
        private const val PLUGIN = "plugin:test"
    }
}
