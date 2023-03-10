/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification

import org.junit.Assert
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.indexmanagement.adminpanel.notification.action.delete.DeleteLRONConfigRequest
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.snapshotmanagement.getRandomString
import org.opensearch.test.OpenSearchTestCase

class SerializationTests : OpenSearchTestCase() {

    fun `test lronConfig serialization`() {
        val lronConfig = randomLRONConfig()
        val out = BytesStreamOutput()
        lronConfig.writeTo(out)

        Assert.assertEquals(
            buildMessage("lronConfig"),
            lronConfig,
            LRONConfig(out.bytes().streamInput())
        )
    }

    fun `test deleteLronConfigRequest`() {
        val deleteLRONConfigRequest = DeleteLRONConfigRequest(getRandomString(20))
        val out = BytesStreamOutput()
        deleteLRONConfigRequest.writeTo(out)
        Assert.assertEquals(
            buildMessage("deleteLronConfigRequest"),
            deleteLRONConfigRequest,
            DeleteLRONConfigRequest(out.bytes().streamInput())
        )
    }

    private fun buildMessage(
        itemType: String
    ): String {
        return "$itemType serialization test failed. "
    }
}
