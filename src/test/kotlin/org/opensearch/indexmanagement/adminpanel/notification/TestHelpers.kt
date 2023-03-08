/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification

import org.opensearch.common.UUIDs
import org.opensearch.tasks.TaskId
import org.opensearch.test.OpenSearchTestCase.randomLong

fun randomTaskId(
    nodeId: String = UUIDs.randomBase64UUID(),
    id: Long = randomLong()
): TaskId {
    return TaskId(nodeId, id)
}
