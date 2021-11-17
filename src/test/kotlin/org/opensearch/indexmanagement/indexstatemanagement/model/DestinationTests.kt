/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Chime
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.CustomWebhook
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Slack
import org.opensearch.test.OpenSearchTestCase

/**
 * Temporary import from alerting, this will be removed once we pull notifications out of
 * alerting so all plugins can consume and use.
 */
class DestinationTests : OpenSearchTestCase() {

    fun `test chime destination`() {
        val chime = Chime("http://abc.com")
        assertEquals("Url is manipulated", chime.url, "http://abc.com")
    }

    fun `test slack destination`() {
        val slack = Slack("http://abc.com")
        assertEquals("Url is manipulated", slack.url, "http://abc.com")
    }

    fun `test custom webhook destination with url and no host`() {
        val customWebhook = CustomWebhook("http://abc.com", null, null, -1, null, emptyMap(), emptyMap(), null, null)
        assertEquals("Url is manipulated", customWebhook.url, "http://abc.com")
    }

    fun `test custom webhook destination with host and no url`() {
        try {
            val customWebhook = CustomWebhook(null, null, "abc.com", 80, null, emptyMap(), emptyMap(), null, null)
            assertEquals("host is manipulated", customWebhook.host, "abc.com")
        } catch (ignored: IllegalArgumentException) {
        }
    }

    fun `test custom webhook destination with url and host`() {
        // In this case, url will be given priority
        val customWebhook = CustomWebhook("http://abc.com", null, null, -1, null, emptyMap(), emptyMap(), null, null)
        assertEquals("Url is manipulated", customWebhook.url, "http://abc.com")
    }

    fun `test custom webhook destination with no url and no host`() {
        try {
            CustomWebhook("", null, null, 80, null, emptyMap(), emptyMap(), null, null)
            fail("Creating a custom webhook destination with empty url did not fail.")
        } catch (ignored: IllegalArgumentException) {
        }
    }
}
