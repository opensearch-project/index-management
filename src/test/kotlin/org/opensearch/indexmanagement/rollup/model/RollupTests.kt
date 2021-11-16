/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.randomSchedule
import org.opensearch.indexmanagement.rollup.randomDateHistogram
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.indexmanagement.rollup.randomTerms
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class RollupTests : OpenSearchTestCase() {
    fun `test rollup same indices`() {
        assertFailsWith(IllegalArgumentException::class, "Your source and target index cannot be the same") {
            randomRollup().copy(sourceIndex = "the_same", targetIndex = "the_same")
        }
    }

    fun `test rollup requires precisely one date histogram`() {
        assertFailsWith(IllegalArgumentException::class, "Must specify precisely one date histogram dimension") {
            randomRollup().copy(dimensions = listOf(randomTerms()))
        }

        assertFailsWith(IllegalArgumentException::class, "Must specify precisely one date histogram dimension") {
            randomRollup().copy(dimensions = emptyList())
        }

        assertFailsWith(IllegalArgumentException::class, "Must specify precisely one date histogram dimension") {
            randomRollup().copy(dimensions = listOf(randomDateHistogram(), randomDateHistogram()))
        }
    }

    fun `test rollup requires first dimension to be date histogram`() {
        assertFailsWith(IllegalArgumentException::class, "The first dimension must be a date histogram") {
            randomRollup().copy(dimensions = listOf(randomTerms(), randomDateHistogram()))
        }
    }

    fun `test rollup requires job enabled time if its enabled`() {
        assertFailsWith(IllegalArgumentException::class, "Job enabled time must be present if the job is enabled") {
            randomRollup().copy(enabled = true, jobEnabledTime = null)
        }
    }

    fun `test rollup requires no job enabled time if its disabled`() {
        assertFailsWith(IllegalArgumentException::class, "Job enabled time must not be present if the job is disabled") {
            randomRollup().copy(enabled = false, jobEnabledTime = randomInstant())
        }
    }

    fun `test rollup requires page size to be between 1 and 10k`() {
        assertFailsWith(IllegalArgumentException::class, "Page size was negative") {
            randomRollup().copy(pageSize = -1)
        }

        assertFailsWith(IllegalArgumentException::class, "Page size was zero") {
            randomRollup().copy(pageSize = 0)
        }

        assertFailsWith(IllegalArgumentException::class, "Page size was 10,0001") {
            randomRollup().copy(pageSize = 10001)
        }

        // These should successfully parse without exceptions
        randomRollup().copy(pageSize = 1)
        randomRollup().copy(pageSize = 10000)
        randomRollup().copy(pageSize = 345)
    }

    fun `test rollup requires delay greater or equal than 0 if set`() {
        assertFailsWith(IllegalArgumentException::class, "Delay was negative") {
            randomRollup().copy(delay = -1)
        }
        assertFailsWith(IllegalArgumentException::class, "Delay was too high") {
            randomRollup().copy(delay = Long.MAX_VALUE)
        }

        // These should successfully parse without exceptions
        randomRollup().copy(delay = 0)
        randomRollup().copy(delay = 930490)
        randomRollup().copy(delay = null)
    }

    fun `test delay applies to continuous rollups only`() {
        // Continuous rollup schedule matches delay
        val newDelay: Long = 500
        val continuousRollup = randomRollup().copy(
            delay = newDelay,
            continuous = true
        )
        assertEquals(newDelay, continuousRollup.jobSchedule.delay)
        // Non continuous rollup schedule should have null delay
        val nonContinuousRollup = randomRollup().copy(
            jobSchedule = randomSchedule(),
            delay = newDelay,
            continuous = false
        )
        assertNull(nonContinuousRollup.jobSchedule.delay)
    }
}
