/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.util

import org.junit.Assert
import org.opensearch.core.tasks.TaskId
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.indexmanagement.controlcenter.notification.randomActionName
import org.opensearch.indexmanagement.controlcenter.notification.randomTaskId
import org.opensearch.test.OpenSearchTestCase
import kotlin.IllegalArgumentException

class LRONUtilsTests : OpenSearchTestCase() {
    fun `test validateActionName`() {
        validateActionName("indices:data/write/reindex")
        Assert.assertThrows(IllegalArgumentException::class.java) {
            validateActionName("indices:data/read/search")
        }
    }

    fun `test validateTaskIdAndActionName`() {
        validateTaskIdAndActionName(randomTaskId(), randomActionName())
        validateTaskIdAndActionName(null, randomActionName())
        validateTaskIdAndActionName(randomTaskId(), null)
        Assert.assertThrows(IllegalArgumentException::class.java) {
            validateTaskIdAndActionName(null, null)
        }
        Assert.assertThrows(IllegalArgumentException::class.java) {
            validateTaskIdAndActionName(randomTaskId(), "indices:data/read/search")
        }
    }

    fun `test getPriority`() {
        Assert.assertEquals(PRIORITY_TASK_ID, getPriority(randomTaskId(), randomActionName()))
        Assert.assertEquals(PRIORITY_DEFAULT_ACTION, getPriority(null, randomActionName()))
    }

    fun `test getDocID`() {
        Assert.assertEquals("LRON:test:123", getDocID(TaskId("test", 123), randomActionName()))
        Assert.assertEquals("LRON:indices:data/write/reindex", getDocID(null, ReindexAction.NAME))
    }
}
