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

package org.opensearch.indexmanagement.rollup.model

import org.opensearch.indexmanagement.rollup.model.RollupFieldMapping.Companion.UNKNOWN_MAPPING
import org.opensearch.test.OpenSearchTestCase

class RollupFieldMappingTests : OpenSearchTestCase() {

    fun `test toIssue`() {
        var fieldMapping = RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, "dummy-field", "terms")
        var actual = fieldMapping.toIssue()
        assertEquals("missing terms grouping on dummy-field", actual)

        actual = fieldMapping.toIssue(true)
        assertEquals("missing field dummy-field", actual)

        fieldMapping = RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, "dummy-field", "sum")
        actual = fieldMapping.toIssue()
        assertEquals("missing sum aggregation on dummy-field", actual)

        actual = fieldMapping.toIssue(true)
        assertEquals("missing field dummy-field", actual)

        fieldMapping = RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, "dummy-field", UNKNOWN_MAPPING)
        actual = fieldMapping.toIssue(false)
        assertEquals("missing field dummy-field", actual)

        actual = fieldMapping.toIssue(true)
        assertEquals("missing field dummy-field", actual)
    }
}
