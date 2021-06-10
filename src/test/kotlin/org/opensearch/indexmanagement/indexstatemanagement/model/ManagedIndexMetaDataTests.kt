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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.io.stream.InputStreamStreamInput
import org.opensearch.common.io.stream.OutputStreamStreamOutput
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StateMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.test.OpenSearchTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

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
