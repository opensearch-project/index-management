/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.opensearchapi

import org.junit.Assert
import org.opensearch.OpenSearchException
import org.opensearch.indexmanagement.util.IndexManagementException
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.TaskCancelledException
import org.opensearch.test.OpenSearchTestCase

class ExtensionsTests : OpenSearchTestCase() {

    fun `test is transform operation timeout`() {
        val ex = OpenSearchException(
            "opensearch test exception",
            TaskCancelledException("cancelled task with reason: Cancellation timeout of 100s is expired")
        )
        val result = isTransformOperationTimedOut(ex)
        Assert.assertTrue(result)
    }

    fun `test is transform operation timeout bad message`() {
        val result = isTransformOperationTimedOut(
            OpenSearchException(
                "opensearch test exception",
                TaskCancelledException("some test msg")
            )
        )
        Assert.assertFalse(result)
    }

    fun `test is retryable`() {
        Assert.assertTrue(isRetryable(IndexManagementException("502", RestStatus.BAD_GATEWAY, RuntimeException()), emptyList()))
        val ex = OpenSearchException(
            "opensearch test exception",
            TaskCancelledException("cancelled task with reason: Cancellation timeout of 100s is expired")
        )
        Assert.assertTrue(isRetryable(ex, emptyList()))
    }
}
