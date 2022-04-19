/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.indexmanagement.indexstatemanagement.randomChannel
import org.opensearch.indexmanagement.indexstatemanagement.randomDestination
import org.opensearch.indexmanagement.indexstatemanagement.randomTemplateScript
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class ErrorNotificationTests : OpenSearchTestCase() {
    fun `test error notification having both channel and destination fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for error notification using both destination and channel") {
            ErrorNotification(destination = randomDestination(), channel = randomChannel(), messageTemplate = randomTemplateScript())
        }
    }

    fun `test error notification having neither channel or destination fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for error notification having neither destination or channel") {
            ErrorNotification(destination = null, channel = null, messageTemplate = randomTemplateScript())
        }
    }

    fun `test error notification having non mustache lang fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for error notification with non mustache lang") {
            ErrorNotification(destination = randomDestination(), channel = null, messageTemplate = randomTemplateScript(lang = "painless"))
        }
    }
}
