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
        // Stop all rollup jobs first to prevent new executions and allow running coroutines to complete
        stopAllRollupJobs()

        // Wait for in-flight job executions to complete their metadata writes
        // This prevents race condition where coroutines recreate config index after wipeAllIndices()
        Thread.sleep(2000)

        // For API tests, flaky could happen if config index not deleted
        // metadata creation could cause the mapping to be auto set to
        //  a wrong type, namely, [rollup_metadata.continuous.next_window_end_time] to long
        wipeAllIndices()
    }
}
