/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.apache.commons.codec.digest.DigestUtils
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.rollup.randomDateHistogram
import org.opensearch.indexmanagement.rollup.randomISMRollup
import org.opensearch.indexmanagement.rollup.randomTerms
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.test.OpenSearchTestCase
import java.time.temporal.ChronoUnit
import kotlin.test.assertFailsWith

class ISMRollupTests : OpenSearchTestCase() {

    fun `test ism rollup requires only one date histogram and it should be first dimension`() {
        assertFailsWith(IllegalArgumentException:: class, "The first dimension must be a date histogram") {
            randomISMRollup().copy(dimensions = listOf(randomTerms(), randomDateHistogram()))
        }

        assertFailsWith(IllegalArgumentException:: class, "Requires one date histogram in dimensions") {
            randomISMRollup().copy(dimensions = listOf())
        }

        assertFailsWith(IllegalArgumentException:: class, "Requires one date histogram in dimensions") {
            randomISMRollup().copy(dimensions = listOf(randomTerms()))
        }

        assertFailsWith(IllegalArgumentException:: class, "Requires only one date histogram in dimensions") {
            randomISMRollup().copy(dimensions = listOf(randomDateHistogram(), randomDateHistogram()))
        }
    }

    fun `test ism rollup requires non empty description`() {
        assertFailsWith(IllegalArgumentException:: class, "Requires non empty description") {
            randomISMRollup().copy(description = "")
        }
    }

    fun `test ism rollup requires non empty target index`() {
        assertFailsWith(IllegalArgumentException:: class, "Requires non empty target index") {
            randomISMRollup().copy(targetIndex = "")
        }
    }

    fun `test ism rollup requires page size to be between 1 and 10K`() {
        assertFailsWith(IllegalArgumentException:: class, "Page size cannot be less than 1") {
            randomISMRollup().copy(pageSize = -1)
        }

        assertFailsWith(IllegalArgumentException:: class, "Page size cannot be less than 1") {
            randomISMRollup().copy(pageSize = 0)
        }

        assertFailsWith(IllegalArgumentException:: class, "Page size cannot be greater than 10000") {
            randomISMRollup().copy(pageSize = 10001)
        }
    }

    fun `test ism toRollup`() {
        val sourceIndex = "dummy-source-index"
        val ismRollup = randomISMRollup()
        val expectedId = DigestUtils.sha1Hex(sourceIndex + ismRollup.toString())
        val rollup = ismRollup.toRollup(sourceIndex)
        val schedule = rollup.schedule as IntervalSchedule

        assertEquals(sourceIndex, rollup.sourceIndex)
        assertEquals(ismRollup.targetIndex, rollup.targetIndex)
        assertEquals(ismRollup.pageSize, rollup.pageSize)
        assertEquals(ismRollup.dimensions, rollup.dimensions)
        assertEquals(ismRollup.metrics, rollup.metrics)
        assertEquals(IndexUtils.DEFAULT_SCHEMA_VERSION, rollup.schemaVersion)
        assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, rollup.seqNo)
        assertEquals(SequenceNumbers.UNASSIGNED_PRIMARY_TERM, rollup.primaryTerm)
        assertEquals(1, schedule.interval)
        assertEquals(ChronoUnit.MINUTES, schedule.unit)
        assertEquals(expectedId, rollup.id)
        assertNull(rollup.metadataID)
        assertNull(rollup.delay)
        assertNotNull(rollup.jobLastUpdatedTime)
        assertNotNull(rollup.jobEnabledTime)
        assertFalse(rollup.continuous)
        assertTrue(rollup.enabled)
        assertTrue(rollup.roles.isEmpty())
        assertTrue(rollup.isEnabled)
    }
}
