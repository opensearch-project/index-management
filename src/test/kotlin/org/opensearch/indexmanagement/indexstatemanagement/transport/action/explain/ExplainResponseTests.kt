/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class ExplainResponseTests : OpenSearchTestCase() {

    fun `test explain response`() {
        val indexNames = listOf("index1")
        val indexPolicyIDs = listOf("policyID1")
        val metadata = ManagedIndexMetaData(
            index = "index1",
            indexUuid = randomAlphaOfLength(10),
            policyID = "policyID1",
            policySeqNo = randomNonNegativeLong(),
            policyPrimaryTerm = randomNonNegativeLong(),
            policyCompleted = null,
            rolledOver = null,
            transitionTo = randomAlphaOfLength(10),
            stateMetaData = null,
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = null,
            info = null
        )
        val indexMetadatas = listOf(metadata)
        val res = ExplainResponse(indexNames, indexPolicyIDs, indexMetadatas)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = ExplainResponse(sin)
        assertEquals(indexNames, newRes.indexNames)
        assertEquals(indexPolicyIDs, newRes.indexPolicyIDs)
        assertEquals(indexMetadatas, newRes.indexMetadatas)
    }

    fun `test explain all response`() {
        val indexNames = listOf("index1")
        val indexPolicyIDs = listOf("policyID1")
        val metadata = ManagedIndexMetaData(
            index = "index1",
            indexUuid = randomAlphaOfLength(10),
            policyID = "policyID1",
            policySeqNo = randomNonNegativeLong(),
            policyPrimaryTerm = randomNonNegativeLong(),
            policyCompleted = null,
            rolledOver = null,
            transitionTo = randomAlphaOfLength(10),
            stateMetaData = null,
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = null,
            info = null
        )
        val indexMetadatas = listOf(metadata)
        val totalManagedIndices = 1
        val enabledState = mapOf("index1" to true)
        val res = ExplainAllResponse(indexNames, indexPolicyIDs, indexMetadatas, totalManagedIndices, enabledState)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = ExplainAllResponse(sin)
        assertEquals(indexNames, newRes.indexNames)
        assertEquals(indexPolicyIDs, newRes.indexPolicyIDs)
        assertEquals(indexMetadatas, newRes.indexMetadatas)
        assertEquals(totalManagedIndices, newRes.totalManagedIndices)
        assertEquals(enabledState, newRes.enabledState)
    }
}
