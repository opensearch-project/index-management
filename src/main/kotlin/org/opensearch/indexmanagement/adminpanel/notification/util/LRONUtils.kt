/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:JvmName("LRONUtils")
package org.opensearch.indexmanagement.adminpanel.notification.util

const val LRON_DEFAULT_ID = "default"
const val LRON_DOC_ID_PREFIX = "LRON:"

const val WITH_PRIORITY = "with_priority"
const val PRIORITY_TASK_ID = 300
const val PRIORITY_DEFAULT_ACTION = 200
const val PRIORITY_DEFAULT = 100
const val DEFAULT_LRON_CONFIG_SORT_FIELD = "lron_config.priority"

fun getPriority(taskId: String? = null, actionName: String? = null): Int {
    return when {
        null != taskId -> PRIORITY_TASK_ID
        null != actionName -> PRIORITY_DEFAULT_ACTION
        else -> PRIORITY_DEFAULT
    }
}

fun getDocID(taskId: String? = null, actionName: String? = null): String {
    if (null != taskId) {
        return LRON_DOC_ID_PREFIX + taskId
    } else if (null != actionName) {
        return LRON_DOC_ID_PREFIX + actionName
    } else return LRON_DOC_ID_PREFIX + LRON_DEFAULT_ID
}
/* if we want to provide default lron config for actions, put const variables here */
