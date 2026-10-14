/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.opensearchapi

import kotlinx.coroutines.suspendCancellableCoroutine
import org.opensearch.action.support.ContextPreservingActionListener
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.identity.PluginSubject
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/** Dispatches as the plugin, restoring the caller's context before delivering either callback. */
internal suspend fun <T> PluginSubject.suspendUntilAsPlugin(
    threadContext: ThreadContext,
    dispatch: (ActionListener<T>) -> Unit,
): T = suspendCancellableCoroutine { continuation ->
    val listener = ContextPreservingActionListener.wrapPreservingContext(
        object : ActionListener<T> {
            override fun onResponse(response: T) = continuation.resume(response)

            override fun onFailure(e: Exception) = continuation.resumeWithException(e)
        },
        threadContext,
    )
    runAs<Exception> { dispatch(listener) }
}
