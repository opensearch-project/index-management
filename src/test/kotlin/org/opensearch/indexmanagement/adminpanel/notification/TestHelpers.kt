/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification

import org.opensearch.common.UUIDs
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.util.getDocID
import org.opensearch.indexmanagement.adminpanel.notification.util.getPriority
import org.opensearch.indexmanagement.adminpanel.notification.util.supportedActions
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.indexmanagement.indexstatemanagement.randomChannel
import org.opensearch.indexmanagement.randomUser
import org.opensearch.tasks.TaskId
import org.opensearch.test.OpenSearchTestCase.*
import org.opensearch.test.rest.OpenSearchRestTestCase

fun randomLRONConfig(
    enabled: Boolean = randomBoolean(),
    taskId: TaskId? = randomTaskId(),
    actionName: String? = randomActionName(),
    channels: List<Channel>? = List(OpenSearchRestTestCase.randomIntBetween(1, 10)) { randomChannel() },
    user: User? = randomUser()
): LRONConfig {
    val priority = getPriority(taskId, actionName)
    return LRONConfig(
        enabled = enabled,
        taskId = taskId,
        actionName = actionName,
        channels = channels,
        user = user,
        priority = priority
    )
}

fun randomTaskId(
    nodeId: String = UUIDs.randomBase64UUID(),
    id: Long = randomLong()
): TaskId {
    return TaskId(nodeId, id)
}

fun randomActionName(): String {
    return supportedActions.random()
}

fun randomLRONConfigResponse(
    version: Long = randomLong(),
    primaryTerm: Long = randomLong(),
    seqNo: Long = randomLong(),
    lronConfig: LRONConfig = randomLRONConfig()
): LRONConfigResponse {
    val id = getDocID(lronConfig.taskId, lronConfig.actionName)
    return LRONConfigResponse(
        id = id,
        version = version,
        primaryTerm = primaryTerm,
        seqNo = seqNo,
        lronConfig = lronConfig
    )
}
