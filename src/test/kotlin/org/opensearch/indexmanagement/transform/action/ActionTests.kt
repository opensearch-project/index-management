/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action

import org.opensearch.indexmanagement.transform.action.delete.DeleteTransformsAction
import org.opensearch.indexmanagement.transform.action.explain.ExplainTransformAction
import org.opensearch.indexmanagement.transform.action.get.GetTransformAction
import org.opensearch.indexmanagement.transform.action.get.GetTransformsAction
import org.opensearch.indexmanagement.transform.action.index.IndexTransformAction
import org.opensearch.indexmanagement.transform.action.preview.PreviewTransformAction
import org.opensearch.indexmanagement.transform.action.start.StartTransformAction
import org.opensearch.indexmanagement.transform.action.stop.StopTransformAction
import org.opensearch.test.OpenSearchTestCase

class ActionTests : OpenSearchTestCase() {

    fun `test delete transform name`() {
        assertNotNull(DeleteTransformsAction.INSTANCE.name())
        assertEquals(DeleteTransformsAction.INSTANCE.name(), DeleteTransformsAction.NAME)
    }

    fun `test index transform name`() {
        assertNotNull(IndexTransformAction.INSTANCE.name())
        assertEquals(IndexTransformAction.INSTANCE.name(), IndexTransformAction.NAME)
    }

    fun `test preview transform name`() {
        assertNotNull(PreviewTransformAction.INSTANCE.name())
        assertEquals(PreviewTransformAction.INSTANCE.name(), PreviewTransformAction.NAME)
    }

    fun `test get transform name`() {
        assertNotNull(GetTransformAction.INSTANCE.name())
        assertEquals(GetTransformAction.INSTANCE.name(), GetTransformAction.NAME)
    }

    fun `test get transforms name`() {
        assertNotNull(GetTransformsAction.INSTANCE.name())
        assertEquals(GetTransformsAction.INSTANCE.name(), GetTransformsAction.NAME)
    }

    fun `test explain transforms name`() {
        assertNotNull(ExplainTransformAction.INSTANCE.name())
        assertEquals(ExplainTransformAction.INSTANCE.name(), ExplainTransformAction.NAME)
    }

    fun `test start transform name`() {
        assertNotNull(StartTransformAction.INSTANCE.name())
        assertEquals(StartTransformAction.INSTANCE.name(), StartTransformAction.NAME)
    }

    fun `test stop transform name`() {
        assertNotNull(StopTransformAction.INSTANCE.name())
        assertEquals(StopTransformAction.INSTANCE.name(), StopTransformAction.NAME)
    }
}
