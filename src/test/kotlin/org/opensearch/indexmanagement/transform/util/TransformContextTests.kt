/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.util

import org.junit.Assert
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.test.OpenSearchTestCase

class TransformContextTests : OpenSearchTestCase() {
    private lateinit var transformLockManager: TransformLockManager
    private lateinit var transformContext: TransformContext

    @Before
    @Throws(Exception::class)
    fun setup() {
        transformLockManager = Mockito.mock(TransformLockManager::class.java)
        transformContext = TransformContext(transformLockManager)
        transformContext.setTargetDateFieldMappings(emptyMap())
    }

    fun `test getMaxRequestTimeoutInSeconds`() {
        val timeout = 1800L
        val expected = 1740L
        Mockito.`when`(transformLockManager.lockExpirationInSeconds()).thenReturn(timeout)
        val result = transformContext.getMaxRequestTimeoutInSeconds()
        Assert.assertNotNull(result)
        assertEquals(expected, result)
    }

    fun `test getMaxRequestTimeoutInSeconds null`() {
        Mockito.`when`(transformLockManager.lockExpirationInSeconds()).thenReturn(null)
        val result = transformContext.getMaxRequestTimeoutInSeconds()
        Assert.assertNull(result)
    }
}
