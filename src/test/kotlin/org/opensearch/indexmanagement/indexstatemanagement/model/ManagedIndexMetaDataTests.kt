/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.io.stream.InputStreamStreamInput
import org.opensearch.common.io.stream.OutputStreamStreamOutput
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.test.OpenSearchTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.time.Instant

class ManagedIndexMetaDataTests : OpenSearchTestCase() {

    fun `test basic`() {
        val expectedManagedIndexMetaData = ManagedIndexMetaData(
            index = "movies",
            indexUuid = "ahPcR4fNRrSe-Q7czV3VPQ",
            policyID = "close_policy",
            policySeqNo = 0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = null,
            indexCreationDate = Instant.now().toEpochMilli(),
            transitionTo = null,
            stateMetaData = StateMetaData("close-index", 1234),
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Successfully initialized policy: close_policy")
        )

        roundTripManagedIndexMetaData(expectedManagedIndexMetaData)
    }

    fun `test action`() {
        val expectedManagedIndexMetaData = ManagedIndexMetaData(
            index = "movies",
            indexUuid = "ahPcR4fNRrSe-Q7czV3VPQ",
            policyID = "close_policy",
            policySeqNo = 0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = null,
            indexCreationDate = null,
            transitionTo = null,
            stateMetaData = StateMetaData("close-index", 1234),
            actionMetaData = ActionMetaData("close", 4321, 0, false, 0, 0, null),
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Successfully closed index")
        )

        roundTripManagedIndexMetaData(expectedManagedIndexMetaData)
    }

    fun `test action property`() {
        val expectedManagedIndexMetaData = ManagedIndexMetaData(
            index = "movies",
            indexUuid = "ahPcR4fNRrSe-Q7czV3VPQ",
            policyID = "close_policy",
            policySeqNo = 0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = null,
            indexCreationDate = null,
            transitionTo = null,
            stateMetaData = StateMetaData("close-index", 1234),
            actionMetaData = ActionMetaData("close", 4321, 0, false, 0, 0, ActionProperties(3)),
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Successfully closed index")
        )

        roundTripManagedIndexMetaData(expectedManagedIndexMetaData)
    }

    fun `test step`() {
        val expectedManagedIndexMetaData = ManagedIndexMetaData(
            index = "movies",
            indexUuid = "ahPcR4fNRrSe-Q7czV3VPQ",
            policyID = "close_policy",
            policySeqNo = 0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = false,
            indexCreationDate = null,
            transitionTo = null,
            stateMetaData = StateMetaData("rollover-index", 1234),
            actionMetaData = ActionMetaData("rollover", 4321, 0, false, 0, 0, null),
            stepMetaData = StepMetaData("attempt_rollover", 6789, Step.StepStatus.FAILED),
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "There is no valid rollover_alias=null set on movies")
        )

        roundTripManagedIndexMetaData(expectedManagedIndexMetaData)
    }

    private fun roundTripManagedIndexMetaData(expectedManagedIndexMetaData: ManagedIndexMetaData) {
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        expectedManagedIndexMetaData.writeTo(osso)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))

        val actualManagedIndexMetaData = ManagedIndexMetaData.fromStreamInput(input)
        for (entry in actualManagedIndexMetaData.toMap()) {
            // Ensure the values are not null.
            // If any of the CustomMetaData map value is null Diffable map will throw an exception.
            assertNotNull("Expected Non null map value.", entry.value)
        }
        assertEquals(expectedManagedIndexMetaData, actualManagedIndexMetaData)
    }
}
