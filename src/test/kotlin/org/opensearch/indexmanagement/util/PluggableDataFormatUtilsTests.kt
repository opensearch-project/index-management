/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.util

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.common.settings.Settings
import org.opensearch.test.OpenSearchTestCase

class PluggableDataFormatUtilsTests : OpenSearchTestCase() {

    fun `test returns true when index has pluggable dataformat enabled`() {
        val indexMetadata: IndexMetadata = mock()
        whenever(indexMetadata.settings).doReturn(
            Settings.builder().put("index.pluggable.dataformat.enabled", true).build(),
        )
        val metadata: Metadata = mock()
        whenever(metadata.index("test-index")).doReturn(indexMetadata)

        assertTrue(PluggableDataFormatUtils.hasPluggableDataFormatEnabled(metadata, arrayOf("test-index")))
    }

    fun `test returns false when index has pluggable dataformat disabled`() {
        val indexMetadata: IndexMetadata = mock()
        whenever(indexMetadata.settings).doReturn(
            Settings.builder().put("index.pluggable.dataformat.enabled", false).build(),
        )
        val metadata: Metadata = mock()
        whenever(metadata.index("test-index")).doReturn(indexMetadata)

        assertFalse(PluggableDataFormatUtils.hasPluggableDataFormatEnabled(metadata, arrayOf("test-index")))
    }

    fun `test returns false when setting is not present`() {
        val indexMetadata: IndexMetadata = mock()
        whenever(indexMetadata.settings).doReturn(Settings.EMPTY)
        val metadata: Metadata = mock()
        whenever(metadata.index("test-index")).doReturn(indexMetadata)

        assertFalse(PluggableDataFormatUtils.hasPluggableDataFormatEnabled(metadata, arrayOf("test-index")))
    }

    fun `test returns false when index metadata is null`() {
        val metadata: Metadata = mock()
        whenever(metadata.index("test-index")).doReturn(null)

        assertFalse(PluggableDataFormatUtils.hasPluggableDataFormatEnabled(metadata, arrayOf("test-index")))
    }

    fun `test returns true when any index in array has pluggable dataformat enabled`() {
        val enabledMetadata: IndexMetadata = mock()
        whenever(enabledMetadata.settings).doReturn(
            Settings.builder().put("index.pluggable.dataformat.enabled", true).build(),
        )
        val disabledMetadata: IndexMetadata = mock()
        whenever(disabledMetadata.settings).doReturn(Settings.EMPTY)

        val metadata: Metadata = mock()
        whenever(metadata.index("idx1")).doReturn(disabledMetadata)
        whenever(metadata.index("idx2")).doReturn(enabledMetadata)

        assertTrue(PluggableDataFormatUtils.hasPluggableDataFormatEnabled(metadata, arrayOf("idx1", "idx2")))
    }

    fun `test returns false for empty indices array`() {
        val metadata: Metadata = mock()
        assertFalse(PluggableDataFormatUtils.hasPluggableDataFormatEnabled(metadata, emptyArray()))
    }
}
