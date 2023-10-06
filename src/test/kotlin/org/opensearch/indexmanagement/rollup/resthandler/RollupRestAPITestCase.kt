/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.resthandler

import org.junit.After
import org.opensearch.indexmanagement.rollup.RollupRestTestCase

abstract class RollupRestAPITestCase : RollupRestTestCase() {
    @After
    fun clearIndicesAfterEachTest() {
        // For API tests, flaky could happen if config index not deleted
        // metadata creation could cause the mapping to be auto set to
        //  a wrong type, namely, [rollup_metadata.continuous.next_window_end_time] to long
        wipeAllIndices()
    }
}
