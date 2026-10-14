/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.fielddomain

import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.identity.PluginSubject
import org.opensearch.indexmanagement.opensearchapi.suspendUntilAsPlugin
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.search.builder.SearchSourceBuilder

/**
 * Search access for field-domain calculators, bound to the managed concrete index.
 *
 * Only internally constructed search sources belong here: searches use the plugin identity to obtain index-wide values.
 * The caller's identity is restored before returning. Failed, timed-out, and early-terminated searches are rejected.
 */
class FieldDomainSearchContext internal constructor(
    private val context: StepContext,
    private val subjectSupplier: () -> PluginSubject = {
        checkNotNull(pluginSubject) { "Field-domain calculation requires a plugin identity" }
    },
) {
    suspend fun search(source: SearchSourceBuilder): SearchResponse {
        val indexName = context.metadata.index
        val request = SearchRequest(indexName)
            .source(source)
            .indicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
            .allowPartialSearchResults(false)
        val subject = subjectSupplier()
        val threadContext = checkNotNull(context.threadContext) { "Field-domain calculation requires a thread context" }
        val response = subject.suspendUntilAsPlugin<SearchResponse>(threadContext) {
            context.client.search(request, it)
        }
        check(!response.isTimedOut) { "Field-domain search timed out for index [$indexName]" }
        check(response.isTerminatedEarly != true) { "Field-domain search terminated early for index [$indexName]" }
        check(response.failedShards == 0 && response.shardFailures.isEmpty()) {
            "Field-domain search failed for index [$indexName], shard failures [${response.shardFailures.joinToString { it.reason() }}]"
        }
        return response
    }

    companion object {
        // The assigned identity assumes one node per plugin classloader.
        @Volatile
        private var pluginSubject: PluginSubject? = null

        // Core assigns the identity after component creation; resolve it when dispatching each search.
        internal fun assignSubject(subject: PluginSubject) {
            pluginSubject = subject
        }
    }
}
