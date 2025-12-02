/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.apache.commons.codec.digest.DigestUtils
import org.opensearch.Version
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.rollup.randomDateHistogram
import org.opensearch.indexmanagement.rollup.randomISMRollup
import org.opensearch.indexmanagement.rollup.randomTerms
import org.opensearch.indexmanagement.rollup.toJsonString
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.test.OpenSearchTestCase
import java.time.temporal.ChronoUnit
import kotlin.test.assertFailsWith

class ISMRollupTests : OpenSearchTestCase() {
    fun `test ism rollup requires only one date histogram and it should be first dimension`() {
        assertFailsWith(IllegalArgumentException::class, "The first dimension must be a date histogram") {
            randomISMRollup().copy(dimensions = listOf(randomTerms(), randomDateHistogram()))
        }

        assertFailsWith(IllegalArgumentException::class, "Requires one date histogram in dimensions") {
            randomISMRollup().copy(dimensions = listOf())
        }

        assertFailsWith(IllegalArgumentException::class, "Requires one date histogram in dimensions") {
            randomISMRollup().copy(dimensions = listOf(randomTerms()))
        }

        assertFailsWith(IllegalArgumentException::class, "Requires only one date histogram in dimensions") {
            randomISMRollup().copy(dimensions = listOf(randomDateHistogram(), randomDateHistogram()))
        }
    }

    fun `test ism rollup requires non empty description`() {
        assertFailsWith(IllegalArgumentException::class, "Requires non empty description") {
            randomISMRollup().copy(description = "")
        }
    }

    fun `test ism rollup requires non empty target index`() {
        assertFailsWith(IllegalArgumentException::class, "Requires non empty target index") {
            randomISMRollup().copy(targetIndex = "")
        }
    }

    fun `test ism rollup requires page size to be between 1 and 10K`() {
        assertFailsWith(IllegalArgumentException::class, "Page size cannot be less than 1") {
            randomISMRollup().copy(pageSize = -1)
        }

        assertFailsWith(IllegalArgumentException::class, "Page size cannot be less than 1") {
            randomISMRollup().copy(pageSize = 0)
        }

        assertFailsWith(IllegalArgumentException::class, "Page size cannot be greater than 10000") {
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
        assertEquals(ismRollup.targetIndexSettings, rollup.targetIndexSettings)
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
        @Suppress("DEPRECATION")
        assertTrue(rollup.roles.isEmpty())
        assertTrue(rollup.isEnabled)
    }

    fun `test ism rollup serialization without source index`() {
        val ismRollup = randomISMRollup().copy(sourceIndex = null)
        val jsonString = ismRollup.toJsonString()

        // Verify source_index is not present in JSON when null
        assertFalse(jsonString.contains("source_index"))

        // Parse back and verify
        val parser = createParser(XContentType.JSON.xContent(), jsonString)
        parser.nextToken()
        val parsed = ISMRollup.parse(parser)

        assertEquals(ismRollup.description, parsed.description)
        assertEquals(ismRollup.targetIndex, parsed.targetIndex)
        assertEquals(ismRollup.pageSize, parsed.pageSize)
        assertEquals(ismRollup.dimensions, parsed.dimensions)
        assertEquals(ismRollup.metrics, parsed.metrics)
        assertNull(parsed.sourceIndex)
    }

    fun `test ism rollup serialization with source index`() {
        val sourceIndex = "my-source-index"
        val ismRollup = randomISMRollup().copy(sourceIndex = sourceIndex)
        val jsonString = ismRollup.toJsonString()

        // Verify source_index is present in JSON
        assertTrue(jsonString.contains("source_index"))
        assertTrue(jsonString.contains(sourceIndex))

        // Parse back and verify
        val parser = createParser(XContentType.JSON.xContent(), jsonString)
        parser.nextToken()
        val parsed = ISMRollup.parse(parser)

        assertEquals(ismRollup.description, parsed.description)
        assertEquals(ismRollup.targetIndex, parsed.targetIndex)
        assertEquals(ismRollup.pageSize, parsed.pageSize)
        assertEquals(ismRollup.dimensions, parsed.dimensions)
        assertEquals(ismRollup.metrics, parsed.metrics)
        assertEquals(sourceIndex, parsed.sourceIndex)
    }

    fun `test ism rollup deserialization without source index field`() {
        val ismRollup = randomISMRollup()
        val jsonString = ismRollup.toJsonString()

        // Manually remove source_index if present to simulate old format
        val cleanedJson = jsonString.replace(Regex(",?\"source_index\":\"[^\"]*\""), "")

        val parser = createParser(XContentType.JSON.xContent(), cleanedJson)
        parser.nextToken()
        val parsed = ISMRollup.parse(parser)

        assertEquals(ismRollup.description, parsed.description)
        assertEquals(ismRollup.targetIndex, parsed.targetIndex)
        assertEquals(ismRollup.pageSize, parsed.pageSize)
        assertNull(parsed.sourceIndex)
    }

    fun `test ism rollup stream serialization without source index on V3_0_0`() {
        val ismRollup = randomISMRollup().copy(sourceIndex = null)

        val output = BytesStreamOutput()
        output.version = Version.V_3_0_0
        ismRollup.writeTo(output)

        val input = StreamInput.wrap(output.bytes().toBytesRef().bytes)
        input.version = Version.V_3_0_0
        val parsed = ISMRollup(input)

        assertEquals(ismRollup.description, parsed.description)
        assertEquals(ismRollup.targetIndex, parsed.targetIndex)
        assertEquals(ismRollup.pageSize, parsed.pageSize)
        assertEquals(ismRollup.dimensions, parsed.dimensions)
        assertEquals(ismRollup.metrics, parsed.metrics)
        assertNull(parsed.sourceIndex)
    }

    fun `test ism rollup stream serialization with source index on V3_0_0`() {
        val sourceIndex = "my-source-index"
        val ismRollup = randomISMRollup().copy(sourceIndex = sourceIndex)

        val output = BytesStreamOutput()
        output.version = Version.V_3_0_0
        ismRollup.writeTo(output)

        val input = StreamInput.wrap(output.bytes().toBytesRef().bytes)
        input.version = Version.V_3_0_0
        val parsed = ISMRollup(input)

        assertEquals(ismRollup.description, parsed.description)
        assertEquals(ismRollup.targetIndex, parsed.targetIndex)
        assertEquals(ismRollup.pageSize, parsed.pageSize)
        assertEquals(ismRollup.dimensions, parsed.dimensions)
        assertEquals(ismRollup.metrics, parsed.metrics)
        assertEquals(sourceIndex, parsed.sourceIndex)
    }

    fun `test ism rollup stream serialization backward compatibility with V2_18_0`() {
        val ismRollup = randomISMRollup().copy(sourceIndex = "my-source-index")

        // Write with V2_18_0 (before source_index support)
        val output = BytesStreamOutput()
        output.version = Version.V_2_18_0
        ismRollup.writeTo(output)

        // Read with V2_18_0 - source_index should not be written
        val input = StreamInput.wrap(output.bytes().toBytesRef().bytes)
        input.version = Version.V_2_18_0
        val parsed = ISMRollup(input)

        assertEquals(ismRollup.description, parsed.description)
        assertEquals(ismRollup.targetIndex, parsed.targetIndex)
        assertEquals(ismRollup.pageSize, parsed.pageSize)
        // sourceIndex should be null when reading from older version
        assertNull(parsed.sourceIndex)
    }

    fun `test ism rollup toRollup with explicit source index`() {
        val explicitSourceIndex = "explicit-source-index"
        val managedIndex = "managed-index"
        val ismRollup = randomISMRollup().copy(sourceIndex = explicitSourceIndex)

        val rollup = ismRollup.toRollup(managedIndex)

        // Should use explicit source_index, not managed index
        assertEquals(explicitSourceIndex, rollup.sourceIndex)
        assertEquals(ismRollup.targetIndex, rollup.targetIndex)

        // Verify ID is computed with explicit source index
        val expectedId = DigestUtils.sha1Hex(explicitSourceIndex + ismRollup.toString())
        assertEquals(expectedId, rollup.id)
    }

    fun `test ism rollup toRollup without explicit source index uses managed index`() {
        val managedIndex = "managed-index"
        val ismRollup = randomISMRollup().copy(sourceIndex = null)

        val rollup = ismRollup.toRollup(managedIndex)

        // Should use managed index as fallback
        assertEquals(managedIndex, rollup.sourceIndex)
        assertEquals(ismRollup.targetIndex, rollup.targetIndex)

        // Verify ID is computed with managed index
        val expectedId = DigestUtils.sha1Hex(managedIndex + ismRollup.toString())
        assertEquals(expectedId, rollup.id)
    }

    fun `test ism rollup null source index handling`() {
        val ismRollup = randomISMRollup().copy(sourceIndex = null)

        // Should not throw exception with null source index
        assertNull(ismRollup.sourceIndex)

        // Should serialize and deserialize correctly
        val jsonString = ismRollup.toJsonString()
        val parser = createParser(XContentType.JSON.xContent(), jsonString)
        parser.nextToken()
        val parsed = ISMRollup.parse(parser)

        assertNull(parsed.sourceIndex)
    }

    fun `test ism rollup with empty string source index is allowed`() {
        // Empty string is technically allowed by the model (validation happens elsewhere)
        val ismRollup = randomISMRollup().copy(sourceIndex = "")

        assertEquals("", ismRollup.sourceIndex)

        // Should serialize and deserialize correctly
        val jsonString = ismRollup.toJsonString()
        val parser = createParser(XContentType.JSON.xContent(), jsonString)
        parser.nextToken()
        val parsed = ISMRollup.parse(parser)

        assertEquals("", parsed.sourceIndex)
    }

    fun `test ism rollup round trip serialization with source index`() {
        val sourceIndex = "test-source-index"
        val original = randomISMRollup().copy(sourceIndex = sourceIndex)

        // XContent round trip
        val jsonString = original.toJsonString()
        val parser = createParser(XContentType.JSON.xContent(), jsonString)
        parser.nextToken()
        val parsedFromXContent = ISMRollup.parse(parser)

        assertEquals(original.description, parsedFromXContent.description)
        assertEquals(original.targetIndex, parsedFromXContent.targetIndex)
        assertEquals(original.sourceIndex, parsedFromXContent.sourceIndex)
        assertEquals(original.pageSize, parsedFromXContent.pageSize)

        // Stream round trip
        val output = BytesStreamOutput()
        output.version = Version.V_3_0_0
        original.writeTo(output)

        val input = StreamInput.wrap(output.bytes().toBytesRef().bytes)
        input.version = Version.V_3_0_0
        val parsedFromStream = ISMRollup(input)

        assertEquals(original.description, parsedFromStream.description)
        assertEquals(original.targetIndex, parsedFromStream.targetIndex)
        assertEquals(original.sourceIndex, parsedFromStream.sourceIndex)
        assertEquals(original.pageSize, parsedFromStream.pageSize)
    }
}
