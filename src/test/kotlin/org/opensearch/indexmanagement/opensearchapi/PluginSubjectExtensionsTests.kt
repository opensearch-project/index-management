/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.opensearchapi

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.opensearch.common.CheckedRunnable
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.identity.PluginSubject
import org.opensearch.test.OpenSearchTestCase
import java.security.Principal
import kotlin.test.assertFailsWith

class PluginSubjectExtensionsTests : OpenSearchTestCase() {
    private val threadContext = ThreadContext(Settings.EMPTY)
    private val subject = object : PluginSubject {
        override fun getPrincipal(): Principal = Principal { "plugin:test" }

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

    fun `test synchronous response restores policy identity`() = runBlocking {
        val result = subject.suspendUntilAsPlugin<String>(threadContext) { listener ->
            assertIdentity(PLUGIN)
            listener.onResponse("bounds")
            assertIdentity(PLUGIN)
        }
        assertEquals("bounds", result)
        assertIdentity(POLICY)
    }

    fun `test delayed response restores policy before continuation`() = runBlocking {
        lateinit var callback: ActionListener<String>
        var resumed = false
        val job = launch(Dispatchers.Unconfined) {
            val result = subject.suspendUntilAsPlugin<String>(threadContext) { listener ->
                assertIdentity(PLUGIN)
                callback = listener
            }
            assertIdentity(POLICY)
            assertEquals("bounds", result)
            resumed = true
        }
        assertFalse(resumed)
        assertIdentity(POLICY)
        subject.runAs<Exception> {
            callback.onResponse("bounds")
            assertIdentity(PLUGIN)
        }
        job.join()
        assertTrue(resumed)
        assertIdentity(POLICY)
    }

    fun `test delayed failure restores policy before continuation`() = runBlocking {
        lateinit var callback: ActionListener<String>
        val failure = IllegalStateException("search failed")
        val job = launch(Dispatchers.Unconfined) {
            val actual = assertFailsWith<IllegalStateException> {
                subject.suspendUntilAsPlugin<String>(threadContext) { listener ->
                    assertIdentity(PLUGIN)
                    callback = listener
                }
            }
            assertEquals(failure.message, actual.message)
            assertIdentity(POLICY)
        }
        assertIdentity(POLICY)
        subject.runAs<Exception> {
            callback.onFailure(failure)
            assertIdentity(PLUGIN)
        }
        job.join()
        assertIdentity(POLICY)
    }

    fun `test dispatch exception restores policy identity`() = runBlocking {
        val failure = IllegalStateException("dispatch failed")
        val actual = assertFailsWith<IllegalStateException> {
            subject.suspendUntilAsPlugin<String>(threadContext) {
                assertIdentity(PLUGIN)
                throw failure
            }
        }
        assertSame(failure, actual)
        assertIdentity(POLICY)
    }

    fun `test cancellation does not leak identity or resume publication`() = runBlocking {
        lateinit var callback: ActionListener<String>
        var resumed = false
        val job = launch(Dispatchers.Unconfined) {
            subject.suspendUntilAsPlugin<String>(threadContext) { listener ->
                assertIdentity(PLUGIN)
                callback = listener
            }
            resumed = true
        }
        job.cancel()
        job.join()
        subject.runAs<Exception> {
            callback.onResponse("late bounds")
            assertIdentity(PLUGIN)
        }
        assertFalse(resumed)
        assertIdentity(POLICY)
    }

    private fun assertIdentity(expected: String) {
        assertEquals(expected, threadContext.getTransient<String>(IDENTITY))
    }

    companion object {
        private const val IDENTITY = "test_identity"
        private const val POLICY = "policy-user"
        private const val PLUGIN = "plugin:test"
    }
}
