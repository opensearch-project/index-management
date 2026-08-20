/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.index.fielddomain.DateRangeFieldDomain
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.fielddomain.AttemptPublishFieldDomainsStep
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class PublishFieldDomainsActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test publish field domains action writes date and date nanos metadata`() {
        val indexName = "${testIndexName}_index"
        val policyID = "${testIndexName}_policy"
        val millisField = "event.millis"
        val nanosField = "event.nanos"
        val millisMin = "2024-05-01T00:00:00.000Z"
        val millisMax = "2024-05-03T12:34:56.789Z"
        val nanosMin = "2024-05-01T00:00:00.000000001Z"
        val nanosMax = "2024-05-01T00:00:00.000000009Z"

        val actionConfig = PublishFieldDomainsAction(
            fields = listOf(
                FieldDomainConfig(millisField, DateRangeFieldDomain.TYPE),
                FieldDomainConfig(nanosField, DateRangeFieldDomain.TYPE),
            ),
            index = 1,
        )
        val states = listOf(
            State(
                name = "PublishFieldDomainsState",
                actions = listOf(ReadOnlyAction(0), actionConfig),
                transitions = listOf(),
            ),
        )
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, replicas = "0", mapping = mapping(millisField, nanosField))
        indexDoc(indexName, source = source(millisMin, nanosMax))
        indexDoc(indexName, source = source(millisMax, nanosMin))
        indexDoc(indexName, source = source("2024-05-02T00:00:00.000Z", "2024-05-01T00:00:00.000000005Z"))
        refresh(indexName)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val metadata = getExplainManagedIndexMetaData(indexName)
            assertEquals(AttemptPublishFieldDomainsStep.getSuccessMessage(indexName, 2), metadata.info?.get("message"))
        }

        val customData = waitFor { getIndexFieldDomains(indexName) }
        assertDateRangeDomain(
            customData = customData,
            field = millisField,
            min = Instant.parse(millisMin).toEpochMilli().toString(),
            max = Instant.parse(millisMax).toEpochMilli().toString(),
            resolution = "milliseconds",
        )
        assertDateRangeDomain(
            customData = customData,
            field = nanosField,
            min = epochNanos(nanosMin),
            max = epochNanos(nanosMax),
            resolution = "nanoseconds",
        )
    }

    private fun mapping(millisField: String, nanosField: String): String =
        """
        "properties": {
          "${millisField.substringBefore(".")}": {
            "properties": {
              "${millisField.substringAfter(".")}": { "type": "date" },
              "${nanosField.substringAfter(".")}": { "type": "date_nanos" }
            }
          }
        }
        """.trimIndent()

    private fun source(millisValue: String, nanosValue: String): String =
        """
        {
          "event": {
            "millis": "$millisValue",
            "nanos": "$nanosValue"
          }
        }
        """.trimIndent()

    @Suppress("UNCHECKED_CAST")
    private fun getIndexFieldDomains(indexName: String): Map<String, String> {
        val response = client().makeRequest(RestRequest.Method.GET.toString(), "/_cluster/state/metadata/$indexName")
        val metadata = response.asMap()["metadata"] as Map<String, Any>
        val indices = metadata["indices"] as Map<String, Any>
        val indexMetadata = indices[indexName] as Map<String, Any>
        return indexMetadata[IndexFieldDomainMetadata.CUSTOM_KEY] as Map<String, String>
    }

    private fun assertDateRangeDomain(
        customData: Map<String, String>,
        field: String,
        min: String,
        max: String,
        resolution: String,
    ) {
        val prefix = "fields.$field."
        assertEquals(DateRangeFieldDomain.TYPE, customData[prefix + "type"])
        assertEquals(min, customData[prefix + "min"])
        assertEquals(max, customData[prefix + "max"])
        assertEquals("true", customData[prefix + "finalized"])
        assertEquals("ism", customData[prefix + "source"])
        assertEquals(resolution, customData[prefix + "resolution"])
    }

    private fun epochNanos(value: String): String {
        val instant = Instant.parse(value)
        return (instant.epochSecond * NANOS_PER_SECOND + instant.nano).toString()
    }

    companion object {
        private const val NANOS_PER_SECOND = 1_000_000_000L
    }
}
