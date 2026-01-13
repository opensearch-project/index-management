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
        val stoppedJobIds = stopAllRollupJobs()

        // Wait for in-flight job executions to actually finish
        // Jobs may be in the middle of processing multiple pages (afterKey loop)
        // They won't check the enabled flag until completing all pages, which can take >2 seconds
        waitForRollupJobsToStop(stoppedJobIds)

        // For API tests, flaky could happen if config index not deleted
        // metadata creation could cause the mapping to be auto set to
        //  a wrong type, namely, [rollup_metadata.continuous.next_window_end_time] to long
        wipeAllIndices()
    }
}
