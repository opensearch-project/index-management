/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.util

/**
 * Temporary import from alerting, this will be removed once we pull notifications out of
 * alerting so all plugins can consume and use.
 */
enum class DestinationType(val value: String) {
    CHIME("chime"),
    SLACK("slack"),
    CUSTOM_WEBHOOK("custom_webhook"),
    TEST_ACTION("test_action")
}
