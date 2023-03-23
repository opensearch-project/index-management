/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.util

import org.junit.Assert
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.indexmanagement.controlcenter.notification.randomActionName
import org.opensearch.indexmanagement.controlcenter.notification.randomTaskId
import org.opensearch.tasks.TaskId
import org.opensearch.test.OpenSearchTestCase

class LRONUtilsTests : OpenSearchTestCase() {
    fun `test validateActionName`() {
        Assert.assertTrue(validateActionName("indices:data/write/reindex"))
        Assert.assertFalse(validateActionName("indices:data/read/search"))
    }

    fun `test getPriority`() {
        Assert.assertEquals(PRIORITY_TASK_ID, getPriority(randomTaskId(), randomActionName()))
        Assert.assertEquals(PRIORITY_DEFAULT_ACTION, getPriority(null, randomActionName()))
        Assert.assertEquals(PRIORITY_DEFAULT, getPriority(null, null))
    }

    fun `test getDocID`() {
        Assert.assertEquals("LRON:test:123", getDocID(TaskId("test", 123), randomActionName()))
        Assert.assertEquals("LRON:indices:data/write/reindex", getDocID(null, ReindexAction.NAME))
        Assert.assertEquals("LRON:default", getDocID(null, null))
    }
}
