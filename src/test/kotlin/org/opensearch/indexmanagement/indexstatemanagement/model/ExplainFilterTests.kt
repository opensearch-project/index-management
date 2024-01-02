/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.test.OpenSearchTestCase

class ExplainFilterTests : OpenSearchTestCase() {
    fun `test byMetadata`() {
        val stateMetaData = StateMetaData("state", 100)
        val actionMetaData = ActionMetaData("action", null, 0, false, 0, null, null)
        val policyRetryInfoMetaData = PolicyRetryInfoMetaData(false, 1)

        val managedIndexMetaDataAllNull = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
        val managedIndexMetaDataNonNull = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, stateMetaData, actionMetaData, null, policyRetryInfoMetaData, null)
        val managedIndexMetaDataSomeNull = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, actionMetaData, null, null, null)

        val explainFilter = ExplainFilter(state = "state", actionType = "action", failed = false)

        var res = explainFilter.byMetaData(managedIndexMetaDataAllNull)
        assertFalse(res)

        res = explainFilter.byMetaData(managedIndexMetaDataNonNull)
        assertTrue(res)

        res = explainFilter.byMetaData(managedIndexMetaDataSomeNull)
        assertFalse(res)
    }
}
