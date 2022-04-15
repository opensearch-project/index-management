/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertFailsWith

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

    fun `test transform page size constraints`() {
        assertFailsWith(IllegalArgumentException::class, "Page size was less than 1") {
            randomTransform().copy(pageSize = -1)
        }

        assertFailsWith(IllegalArgumentException::class, "Page size was greater than 10K") {
            randomTransform().copy(continuous = false, pageSize = 10001)
        }

        assertFailsWith(IllegalArgumentException::class, "Page size was greater than 1K") {
            randomTransform().copy(continuous = true, pageSize = 1001)
        }

        randomTransform().copy(pageSize = 1)
        randomTransform().copy(continuous = false, pageSize = 10000)
        randomTransform().copy(continuous = true, pageSize = 1000)
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
