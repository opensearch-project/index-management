/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action

import org.opensearch.indexmanagement.rollup.action.delete.DeleteRollupAction
import org.opensearch.indexmanagement.rollup.action.get.GetRollupAction
import org.opensearch.indexmanagement.rollup.action.get.GetRollupsAction
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupAction
import org.opensearch.indexmanagement.rollup.action.start.StartRollupAction
import org.opensearch.indexmanagement.rollup.action.stop.StopRollupAction
import org.opensearch.test.OpenSearchTestCase

class ActionTests : OpenSearchTestCase() {

    fun `test delete action name`() {
        assertNotNull(DeleteRollupAction.INSTANCE.name())
        assertEquals(DeleteRollupAction.INSTANCE.name(), DeleteRollupAction.NAME)
    }

    fun `test index action name`() {
        assertNotNull(IndexRollupAction.INSTANCE.name())
        assertEquals(IndexRollupAction.INSTANCE.name(), IndexRollupAction.NAME)
    }

    fun `test get action name`() {
        assertNotNull(GetRollupAction.INSTANCE.name())
        assertEquals(GetRollupAction.INSTANCE.name(), GetRollupAction.NAME)
    }

    fun `test get(s) action name`() {
        assertNotNull(GetRollupsAction.INSTANCE.name())
        assertEquals(GetRollupsAction.INSTANCE.name(), GetRollupsAction.NAME)
    }

    fun `test start action name`() {
        assertNotNull(StartRollupAction.INSTANCE.name())
        assertEquals(StartRollupAction.INSTANCE.name(), StartRollupAction.NAME)
    }

    fun `test stop action name`() {
        assertNotNull(StopRollupAction.INSTANCE.name())
        assertEquals(StopRollupAction.INSTANCE.name(), StopRollupAction.NAME)
    }
}
