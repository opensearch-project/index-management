/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.notification

import org.opensearch.indexmanagement.notification.model.NotificationConf

class NotificationService {

    public fun getNotificationConfByAction(action: String): NotificationConf? {
        // TODO check notification channel existence
        return NotificationConf("SsPLbYYBwCDAm0J8Rfsr", action)
    }
}