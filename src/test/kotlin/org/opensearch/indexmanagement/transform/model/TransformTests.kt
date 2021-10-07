/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.indexmanagement.rollup.randomDateHistogram
import org.opensearch.indexmanagement.rollup.randomTerms
import org.opensearch.indexmanagement.transform.randomGroups
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

    fun `test transform requirements when continuous approach is incremental`() {
        val dateDimension = randomDateHistogram()
        val otherDimension = randomTerms()
        val groups = randomGroups().plus(dateDimension).plus(otherDimension)
        var continuous = ContinuousInfo(ContinuousInfo.ContinuousApproach.INCREMENTAL, "will_fail")
        assertFailsWith(IllegalArgumentException::class, "Continuous time field must match a dimension field") {
            randomTransform().copy(groups = groups, continuous = continuous)
        }
        continuous = ContinuousInfo(ContinuousInfo.ContinuousApproach.INCREMENTAL, otherDimension.sourceField)
        assertFailsWith(IllegalArgumentException::class, "Dimension noted by the time field must be a date histogram") {
            randomTransform().copy(groups = groups, continuous = continuous)
        }

        continuous = ContinuousInfo(ContinuousInfo.ContinuousApproach.INCREMENTAL, dateDimension.sourceField)
        randomTransform().copy(groups = groups, continuous = continuous)
        randomTransform().copy(continuous = ContinuousInfo(ContinuousInfo.ContinuousApproach.SIMPLE, null))
        randomTransform().copy(continuous = null)
    }
}
