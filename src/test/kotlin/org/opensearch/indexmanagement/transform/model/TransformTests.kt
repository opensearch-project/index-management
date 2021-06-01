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

package org.opensearch.indexmanagement.transform.model

import java.time.Instant
import java.time.temporal.ChronoUnit
import org.opensearch.indexmanagement.transform.randomTransform
import kotlin.test.assertFailsWith
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.test.OpenSearchTestCase

class TransformTests : OpenSearchTestCase() {

    fun `test transform same indices`() {
        assertFailsWith(IllegalArgumentException::class, "Source and target index cannot be the same") {
            randomTransform().copy(sourceIndex = "dummy-index", targetIndex = "dummy-index")
        }
    }

    fun `test transform requires at least one grouping`() {
        assertFailsWith(IllegalArgumentException::class, "Must specify at least one grouping") {
            randomTransform().copy(groups = listOf())
        }
    }

    fun `test transform requires page size to be between 1 and 10K`() {
        assertFailsWith(IllegalArgumentException::class, "Page size was less than 1") {
            randomTransform().copy(pageSize = -1)
        }

        assertFailsWith(IllegalArgumentException::class, "Page size was greater than 10K") {
            randomTransform().copy(pageSize = 10001)
        }

        randomTransform().copy(pageSize = 1)
        randomTransform().copy(pageSize = 10000)
        randomTransform().copy(pageSize = 500)
    }

    fun `test transform requires interval schedule period to be greater than 0`() {
        val schedule = IntervalSchedule(Instant.now(), 0, ChronoUnit.HOURS)
        assertFailsWith(IllegalArgumentException::class, "Period was not greater than 0") {
            randomTransform().copy(jobSchedule = schedule)
        }

        randomTransform().copy(jobSchedule = IntervalSchedule(Instant.now(), 2, ChronoUnit.HOURS))
    }
}
