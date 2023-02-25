/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.notification.model

import org.opensearch.script.Script

class NotificationConf(var channelId: String, var action: String) {

    private var template: Script? = null
    private var taskId: String? = null // task level configuration

    public constructor(channelId: String, action: String, template: Script?, taskId: String) : this(channelId, action) {
        this.template = template
        this.taskId = taskId
    }
}