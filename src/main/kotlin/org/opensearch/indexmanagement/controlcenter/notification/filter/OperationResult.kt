/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter

enum class OperationResult(val desc: String) {
    COMPLETE("completed"), FAILED("failed"), TIMEOUT("failed")
}
