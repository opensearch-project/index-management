/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification

import org.junit.Assert
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.indexmanagement.controlcenter.notification.action.delete.DeleteLRONConfigRequest
import org.opensearch.indexmanagement.controlcenter.notification.action.get.GetLRONConfigRequest
import org.opensearch.indexmanagement.controlcenter.notification.action.get.GetLRONConfigResponse
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.opensearchapi.convertToMap
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
            LRONConfig(out.bytes().streamInput()),
        )
    }

    fun `test deleteLRONConfigRequest`() {
        val deleteLRONConfigRequest = DeleteLRONConfigRequest(getRandomString(20))
        val out = BytesStreamOutput()
        deleteLRONConfigRequest.writeTo(out)
        Assert.assertEquals(
            buildMessage("deleteLronConfigRequest"),
            deleteLRONConfigRequest.docId,
            DeleteLRONConfigRequest(out.bytes().streamInput()).docId,
        )
    }

    fun `test getLRONConfigRequest`() {
        val getLRONConfigRequest = GetLRONConfigRequest(getRandomString(20))
        val out = BytesStreamOutput()
        getLRONConfigRequest.writeTo(out)
        Assert.assertEquals(
            buildMessage("getLronConfigRequest"),
            getLRONConfigRequest.docId,
            GetLRONConfigRequest(out.bytes().streamInput()).docId,
        )
    }

    fun `test lronConfigResponse`() {
        val lronConfigResponse = randomLRONConfigResponse()
        val out = BytesStreamOutput()
        lronConfigResponse.writeTo(out)
        Assert.assertEquals(
            buildMessage("lronConfigResponse"),
            lronConfigResponse.convertToMap(),
            LRONConfigResponse(out.bytes().streamInput()).convertToMap(),
        )
    }

    fun `test getLRONConfigResponse`() {
        val getLRONConfigResponse = randomGetLRONConfigResponse(10)
        val out = BytesStreamOutput()
        getLRONConfigResponse.writeTo(out)
        Assert.assertEquals(
            buildMessage("getLRONConfigResponse"),
            getLRONConfigResponse.convertToMap(),
            GetLRONConfigResponse(out.bytes().streamInput()).convertToMap(),
        )
    }

    private fun buildMessage(
        itemType: String,
    ): String {
        return "$itemType serialization test failed. "
    }
}
