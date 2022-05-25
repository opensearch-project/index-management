/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.action

import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.DELETE_SM_POLICY_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.DELETE_SM_POLICY_ACTION_TYPE
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICY_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICY_ACTION_TYPE
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.INDEX_SM_POLICY_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.INDEX_SM_POLICY_ACTION_TYPE
import org.opensearch.test.OpenSearchTestCase

class ActionTests : OpenSearchTestCase() {

    fun `test delete action name`() {
        assertNotNull(DELETE_SM_POLICY_ACTION_TYPE.name())
        assertEquals(DELETE_SM_POLICY_ACTION_TYPE.name(), DELETE_SM_POLICY_ACTION_NAME)
    }

    fun `test index action name`() {
        assertNotNull(INDEX_SM_POLICY_ACTION_TYPE.name())
        assertEquals(INDEX_SM_POLICY_ACTION_TYPE.name(), INDEX_SM_POLICY_ACTION_NAME)
    }

    fun `test get action name`() {
        assertNotNull(GET_SM_POLICY_ACTION_TYPE.name())
        assertEquals(GET_SM_POLICY_ACTION_TYPE.name(), GET_SM_POLICY_ACTION_NAME)
    }
}
