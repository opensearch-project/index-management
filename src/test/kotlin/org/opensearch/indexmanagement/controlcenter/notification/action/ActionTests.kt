/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action

import org.opensearch.indexmanagement.controlcenter.notification.action.delete.DeleteLRONConfigAction
import org.opensearch.indexmanagement.controlcenter.notification.action.get.GetLRONConfigAction
import org.opensearch.indexmanagement.controlcenter.notification.action.index.IndexLRONConfigAction
import org.opensearch.test.OpenSearchTestCase

class ActionTests : OpenSearchTestCase() {
    fun `test index lronConfig action name`() {
        assertNotNull(IndexLRONConfigAction.INSTANCE.name())
        assertEquals(IndexLRONConfigAction.INSTANCE.name(), IndexLRONConfigAction.NAME)
    }

    fun `test delete lronConfig action name`() {
        assertNotNull(DeleteLRONConfigAction.INSTANCE.name())
        assertEquals(DeleteLRONConfigAction.INSTANCE.name(), DeleteLRONConfigAction.NAME)
    }

    fun `test get lronConfig action name`() {
        assertNotNull(GetLRONConfigAction.INSTANCE.name())
        assertEquals(GetLRONConfigAction.INSTANCE.name(), GetLRONConfigAction.NAME)
    }
}
