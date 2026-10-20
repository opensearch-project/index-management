/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.fielddomain

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.index.Index
import org.opensearch.index.fielddomain.DateRangeFieldDomain
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainConfig
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class ComputeFieldDomainsRequestTests : OpenSearchTestCase() {
    private val index = Index("logs-000001", "index-uuid")
    private val fields = listOf(FieldDomainConfig("@timestamp", DateRangeFieldDomain.TYPE))

    fun `test request round trip preserves exact index identity and fields`() {
        val request = ComputeFieldDomainsRequest(index, fields)
        request.setParentTask("coordinator", 42L)
        BytesStreamOutput().use { out ->
            request.writeTo(out)
            out.bytes().streamInput().use { input ->
                val copy = ComputeFieldDomainsRequest(input)
                assertEquals(index, copy.targetIndex)
                assertEquals(fields, copy.fields)
                assertEquals(request.parentTask, copy.parentTask)
                assertArrayEquals(arrayOf(index.name), copy.indices())
                assertEquals(request.indicesOptions(), copy.indicesOptions())
                assertNull(copy.validate())
                assertEquals(0, input.available())
            }
        }
    }

    fun `test security resolver cannot change the authorized index`() {
        val request = ComputeFieldDomainsRequest(index, fields)
        assertSame(request, request.indices(index.name))
        for (names in listOf(emptyArray(), arrayOf("other"), arrayOf("*"), arrayOf(index.name, "other"))) {
            assertFailsWith<IllegalArgumentException> { request.indices(*names) }
            assertEquals(index, request.targetIndex)
        }
        val names = request.indices()
        names[0] = "other"
        assertArrayEquals(arrayOf(index.name), request.indices())
    }

    fun `test request validation rejects missing identity empty fields and duplicates`() {
        assertNotNull(ComputeFieldDomainsRequest(Index("", "uuid"), fields).validate())
        assertNotNull(ComputeFieldDomainsRequest(Index(index.name, "_na_"), fields).validate())
        assertNotNull(ComputeFieldDomainsRequest(Index(index.name, ""), fields).validate())
        assertNotNull(ComputeFieldDomainsRequest(index, emptyList()).validate())
        assertNotNull(ComputeFieldDomainsRequest(index, fields + fields).validate())
    }

    fun `test request defensively copies configurations`() {
        val configurations = fields.toMutableList()
        val request = ComputeFieldDomainsRequest(index, configurations)
        configurations.clear()
        assertEquals(fields, request.fields)
    }

    fun `test response round trip preserves domain metadata and empty results`() {
        for (domains in listOf(emptyList(), listOf(DateRangeFieldDomain("@timestamp", 1L, 2L, true, "ism")))) {
            val metadata = IndexFieldDomainMetadata.getInstance().toCustomData(domains)
            BytesStreamOutput().use { out ->
                ComputeFieldDomainsResponse(metadata).writeTo(out)
                out.bytes().streamInput().use { input ->
                    assertEquals(metadata, IndexFieldDomainMetadata.getInstance().toCustomData(ComputeFieldDomainsResponse(input).fieldDomains()))
                    assertEquals(0, input.available())
                }
            }
        }
    }
}
