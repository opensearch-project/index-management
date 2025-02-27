/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.indexmanagement.util

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.identity.Subject
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.FilterClient

/**
 * Implementation of client that will run transport actions in a stashed context and inject the name of the provided
 * subject into the context.
 */
@Suppress("TooGenericExceptionThrown")
class RunAsSubjectClient : FilterClient {
    private var subject: Subject? = null

    constructor(delegate: Client) : super(delegate)

    constructor(delegate: Client, subject: Subject?) : super(delegate) {
        this.subject = subject
    }

    fun setSubject(subject: Subject?) {
        this.subject = subject
    }

    override fun <Request : ActionRequest?, Response : ActionResponse?> doExecute(
        action: ActionType<Response>,
        request: Request,
        listener: ActionListener<Response>,
    ) {
        checkNotNull(subject) { "RunAsSubjectClient is not initialized." }
        try {
            threadPool().threadContext.newStoredContext(false).use { ctx ->
                subject!!.runAs<Any?> {
                    Companion.logger.info(
                        "Running transport action with subject: {}",
                        subject!!.principal.name,
                    )
                    super.doExecute(
                        action, request,
                        ActionListener.runBefore(
                            listener,
                        ) { ctx.restore() },
                    )
                    null
                }
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(
            RunAsSubjectClient::class.java,
        )
    }
}
