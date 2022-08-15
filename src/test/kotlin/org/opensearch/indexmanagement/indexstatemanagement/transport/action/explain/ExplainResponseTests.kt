/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ValidationResult
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
            indexCreationDate = null,
            transitionTo = randomAlphaOfLength(10),
            stateMetaData = null,
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = null,
            info = null
        )
        val validationResult = ValidationResult("test", Validate.ValidationStatus.FAILED)
        val validationResults = listOf(validationResult)
        val indexMetadatas = listOf(metadata)
        val totalManagedIndices = 1
        val enabledState = mapOf("index1" to true)
        val appliedPolicies = mapOf("policy" to randomPolicy())
        val res = ExplainResponse(indexNames, indexPolicyIDs, indexMetadatas, totalManagedIndices, enabledState, appliedPolicies, validationResults)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = ExplainResponse(sin)
        assertEquals(indexNames, newRes.indexNames)
        assertEquals(indexPolicyIDs, newRes.indexPolicyIDs)
        assertEquals(indexMetadatas, newRes.indexMetadatas)
        assertEquals(totalManagedIndices, newRes.totalManagedIndices)
        assertEquals(enabledState, newRes.enabledState)
        assertEquals(appliedPolicies, newRes.policies)
        assertEquals(validationResults, newRes.validationResults)
    }
}
