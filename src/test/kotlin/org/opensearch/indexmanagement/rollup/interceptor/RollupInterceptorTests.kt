/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.Companion.BYPASS_ROLLUP_SEARCH
import org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.Companion.BYPASS_SIZE_CHECK
import org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.Companion.clearBypass
import org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.Companion.getBypassLevel
import org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.Companion.setBypass
import org.opensearch.test.OpenSearchTestCase

class RollupInterceptorTests : OpenSearchTestCase() {

    fun `test setBypass and getBypassLevel for BYPASS_ROLLUP_SEARCH`() {
        setBypass(BYPASS_ROLLUP_SEARCH)
        assertEquals(BYPASS_ROLLUP_SEARCH, getBypassLevel())
        clearBypass()
    }

    fun `test setBypass and getBypassLevel for BYPASS_SIZE_CHECK`() {
        setBypass(BYPASS_SIZE_CHECK)
        assertEquals(BYPASS_SIZE_CHECK, getBypassLevel())
        clearBypass()
    }

    fun `test getBypassLevel returns 0 when not set`() {
        clearBypass()
        assertEquals(0, getBypassLevel())
    }

    fun `test clearBypass resets bypass level`() {
        setBypass(BYPASS_ROLLUP_SEARCH)
        assertEquals(BYPASS_ROLLUP_SEARCH, getBypassLevel())
        clearBypass()
        assertEquals(0, getBypassLevel())
    }

    fun `test bypass is thread local`() {
        setBypass(BYPASS_ROLLUP_SEARCH)
        assertEquals(BYPASS_ROLLUP_SEARCH, getBypassLevel())

        val thread = Thread {
            assertEquals(0, getBypassLevel())
            setBypass(BYPASS_SIZE_CHECK)
            assertEquals(BYPASS_SIZE_CHECK, getBypassLevel())
        }
        thread.start()
        thread.join()

        assertEquals(BYPASS_ROLLUP_SEARCH, getBypassLevel())
        clearBypass()
    }

    fun `test multiple setBypass calls overwrite previous value`() {
        setBypass(BYPASS_ROLLUP_SEARCH)
        assertEquals(BYPASS_ROLLUP_SEARCH, getBypassLevel())
        setBypass(BYPASS_SIZE_CHECK)
        assertEquals(BYPASS_SIZE_CHECK, getBypassLevel())
        clearBypass()
    }
}
