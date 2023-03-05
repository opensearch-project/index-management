/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:JvmName("LRONUtils")
package org.opensearch.indexmanagement.adminpanel.notification.util

const val LRON_CONFIG_TYPE = "lron_config_type"
const val LRON_DEFAULT_ID = "default"
const val LRON_DOC_ID_PREFIX = "LRON:"

const val LRON_TYPE_TASK_ID = "task_id"
const val LRON_TYPE_DEFAULT = "default"

fun getDocID(taskId: String? = null, actionName: String? = null): String {
    if (null != taskId) {
        return LRON_DOC_ID_PREFIX + taskId
    } else if (null != actionName) {
        return LRON_DOC_ID_PREFIX + actionName
    } else return LRON_DOC_ID_PREFIX + LRON_DEFAULT_ID
}
/* if we want to provide default lron config for actions, put const variables here */
