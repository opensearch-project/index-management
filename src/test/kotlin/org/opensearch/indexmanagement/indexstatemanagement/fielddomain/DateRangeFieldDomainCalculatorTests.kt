/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.fielddomain

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.Version
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.MappingMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.index.fielddomain.DateRangeFieldDomain
import org.opensearch.index.mapper.DateFieldMapper
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.search.DocValueFormat
import org.opensearch.search.SearchHit
import org.opensearch.search.SearchHits
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.Client
import java.time.Instant
import java.util.ArrayDeque
import kotlin.test.assertFailsWith

class DateRangeFieldDomainCalculatorTests : OpenSearchTestCase() {
    private val calculator = DateRangeFieldDomainCalculator()

    fun `test calculator preserves date raw sort values as milliseconds`() {
        val domain = calculate(
            fieldType = DateFieldMapper.CONTENT_TYPE,
            client = client(searchResponse(1_000L), searchResponse(2_000L)),
        )

        assertEquals("@timestamp", domain.field())
        assertEquals("1000", domain.min())
        assertEquals("2000", domain.max())
        assertEquals("milliseconds", domain.resolution())
        assertTrue(domain.finalized())
    }

    fun `test calculator preserves date nanos raw sort values as nanoseconds`() {
        val domain = calculate(
            fieldType = DateFieldMapper.DATE_NANOS_CONTENT_TYPE,
            client = client(searchResponse(1_000_000_001L), searchResponse(2_000_000_009L)),
        )

        assertEquals("@timestamp", domain.field())
        assertEquals("1000000001", domain.min())
        assertEquals("2000000009", domain.max())
        assertEquals("nanoseconds", domain.resolution())
        assertTrue(domain.finalized())
    }

    fun `test calculator returns null when field has no values`() {
        val domain = runBlocking {
            calculator.calculate(
                context(client(emptySearchResponse())),
                indexMetadata(fieldType = DateFieldMapper.CONTENT_TYPE),
                FieldDomainConfig("@timestamp", DateRangeFieldDomain.TYPE),
                finalized = true,
            )
        }

        assertNull(domain)
    }

    fun `test calculator rejects unsupported mapping type`() {
        val exception = assertFailsWith<IllegalArgumentException> {
            runBlocking {
                calculator.calculate(
                    context(mock<Client>()),
                    indexMetadata(fieldType = "keyword"),
                    FieldDomainConfig("@timestamp", DateRangeFieldDomain.TYPE),
                    finalized = true,
                )
            }
        }

        assertTrue(exception.message!!.contains("must be mapped as [date] or [date_nanos]"))
    }

    fun `test calculator rejects missing field mapping`() {
        val exception = assertFailsWith<IllegalArgumentException> {
            runBlocking {
                calculator.calculate(
                    context(mock<Client>()),
                    indexMetadata(field = "event.created", fieldType = DateFieldMapper.CONTENT_TYPE),
                    FieldDomainConfig("@timestamp", DateRangeFieldDomain.TYPE),
                    finalized = true,
                )
            }
        }

        assertEquals("Field [@timestamp] is not present in index [$INDEX_NAME] mappings", exception.message)
    }

    private fun calculate(fieldType: String, client: Client): DateRangeFieldDomain {
        val domain = runBlocking {
            calculator.calculate(
                context(client),
                indexMetadata(fieldType = fieldType),
                FieldDomainConfig("@timestamp", DateRangeFieldDomain.TYPE),
                finalized = true,
            )
        }
        assertNotNull(domain)
        return domain as DateRangeFieldDomain
    }

    private fun context(client: Client): StepContext = StepContext(
        managedIndexMetaData(),
        mock(),
        client,
        null,
        null,
        mock<ScriptService>(),
        Settings.EMPTY,
        mock<LockService>(),
    )

    private fun indexMetadata(
        field: String = "@timestamp",
        fieldType: String,
    ): IndexMetadata = IndexMetadata.Builder(INDEX_NAME)
        .settings(
            settings(Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, INDEX_UUID)
                .build(),
        )
        .numberOfShards(1)
        .numberOfReplicas(0)
        .putMapping(MappingMetadata("_doc", mapping(field, fieldType)))
        .build()

    private fun mapping(field: String, fieldType: String): Map<String, Any> {
        var properties: Map<String, Any> = mapOf("type" to fieldType)
        field.split(".").asReversed().forEach { fieldPart ->
            properties = mapOf("properties" to mapOf(fieldPart to properties))
        }
        return properties
    }

    private fun managedIndexMetaData(): ManagedIndexMetaData = ManagedIndexMetaData(
        INDEX_NAME,
        INDEX_UUID,
        "policy_id",
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        ActionMetaData("publish_field_domains", Instant.now().toEpochMilli(), 0, false, 1, null, null),
        null,
        null,
        null,
    )

    private fun client(vararg searchResponses: SearchResponse): Client {
        val responses = ArrayDeque(searchResponses.toList())
        return mock {
            doAnswer { invocation ->
                val listener = invocation.getArgument<ActionListener<SearchResponse>>(1)
                listener.onResponse(responses.removeFirst())
            }.whenever(this.mock).search(any(), any())
        }
    }

    private fun searchResponse(rawSortValue: Long): SearchResponse {
        val searchHit = SearchHit(0)
        searchHit.sortValues(arrayOf<Any>(rawSortValue), arrayOf(DocValueFormat.RAW))
        return searchResponse(SearchHits(arrayOf(searchHit), null, 0.0F))
    }

    private fun emptySearchResponse(): SearchResponse = searchResponse(SearchHits(emptyArray(), null, 0.0F))

    private fun searchResponse(searchHits: SearchHits): SearchResponse {
        val searchResponse: SearchResponse = mock()
        whenever(searchResponse.hits).doReturn(searchHits)
        whenever(searchResponse.shardFailures).doReturn(ShardSearchFailure.EMPTY_ARRAY)
        return searchResponse
    }

    companion object {
        private const val INDEX_NAME = "test-index"
        private const val INDEX_UUID = "test-index-uuid"
    }
}
