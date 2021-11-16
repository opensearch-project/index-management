/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class ManagedIndexConfigTests : OpenSearchTestCase() {

    fun `test managed index config parsing`() {

        val missingIndexUuid = """{"managed_index":{"name":"edpNNwdVXG","enabled":false,"index":"DcdVHfmQUI","schedule":{"interval":{"start_time":1560402722674,"period":5,"unit":"Minutes"}},"last_updated_time":1560402722676,"enabled_time":null,"policy_id":"KumaJGCWPi","policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"error_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","action":[],"transitions":[]},{"name":"IIJxQdcenu","action":[],"transitions":[]},{"name":"zSXlbLUBqG","action":[],"transitions":[]},{"name":"nYRPBojBiy","action":[],"transitions":[]}]},"change_policy":{"policy_id":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""
        val missingIndex = """{"managed_index":{"name":"edpNNwdVXG","enabled":false,"index_uuid":"SdcNvtdyAZYyrVkFMoQr","schedule":{"interval":{"start_time":1560402722674,"period":5,"unit":"Minutes"}},"last_updated_time":1560402722676,"enabled_time":null,"policy_id":"KumaJGCWPi","policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"error_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","action":[],"transitions":[]},{"name":"IIJxQdcenu","action":[],"transitions":[]},{"name":"zSXlbLUBqG","action":[],"transitions":[]},{"name":"nYRPBojBiy","action":[],"transitions":[]}]},"change_policy":{"policy_id":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""
        val missingName = """{"managed_index":{"enabled":false,"index":"DcdVHfmQUI","index_uuid":"SdcNvtdyAZYyrVkFMoQr","schedule":{"interval":{"start_time":1560402722674,"period":5,"unit":"Minutes"}},"last_updated_time":1560402722676,"enabled_time":null,"policy_id":"KumaJGCWPi","policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"error_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","action":[],"transitions":[]},{"name":"IIJxQdcenu","action":[],"transitions":[]},{"name":"zSXlbLUBqG","action":[],"transitions":[]},{"name":"nYRPBojBiy","action":[],"transitions":[]}]},"change_policy":{"policy_id":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""
        val missingSchedule = """{"managed_index":{"name":"edpNNwdVXG","enabled":false,"index":"DcdVHfmQUI","index_uuid":"SdcNvtdyAZYyrVkFMoQr","last_updated_time":1560402722676,"enabled_time":null,"policy_id":"KumaJGCWPi","policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"error_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","action":[],"transitions":[]},{"name":"IIJxQdcenu","action":[],"transitions":[]},{"name":"zSXlbLUBqG","action":[],"transitions":[]},{"name":"nYRPBojBiy","action":[],"transitions":[]}]},"change_policy":{"policy_id":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""
        val missingLastUpdatedTime = """{"managed_index":{"name":"edpNNwdVXG","enabled":false,"index":"DcdVHfmQUI","index_uuid":"SdcNvtdyAZYyrVkFMoQr","schedule":{"interval":{"start_time":1560402722674,"period":5,"unit":"Minutes"}},"enabled_time":null,"policy_id":"KumaJGCWPi","policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"error_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","action":[],"transitions":[]},{"name":"IIJxQdcenu","action":[],"transitions":[]},{"name":"zSXlbLUBqG","action":[],"transitions":[]},{"name":"nYRPBojBiy","action":[],"transitions":[]}]},"change_policy":{"policy_id":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""
        val missingPolicyID = """{"managed_index":{"name":"edpNNwdVXG","enabled":false,"index":"DcdVHfmQUI","index_uuid":"SdcNvtdyAZYyrVkFMoQr","schedule":{"interval":{"start_time":1560402722674,"period":5,"unit":"Minutes"}},"last_updated_time":1560402722676,"enabled_time":null,"policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"error_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","action":[],"transitions":[]},{"name":"IIJxQdcenu","action":[],"transitions":[]},{"name":"zSXlbLUBqG","action":[],"transitions":[]},{"name":"nYRPBojBiy","action":[],"transitions":[]}]},"change_policy":{"policy_id":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""

        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing indexUuid") {
            parserWithType(missingIndexUuid).parseWithType(parse = ManagedIndexConfig.Companion::parse)
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing index") {
            parserWithType(missingIndex).parseWithType(parse = ManagedIndexConfig.Companion::parse)
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing name") {
            parserWithType(missingName).parseWithType(parse = ManagedIndexConfig.Companion::parse)
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing schedule") {
            parserWithType(missingSchedule).parseWithType(parse = ManagedIndexConfig.Companion::parse)
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing lastUpdatedTime") {
            parserWithType(missingLastUpdatedTime).parseWithType(parse = ManagedIndexConfig.Companion::parse)
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing policyID") {
            parserWithType(missingPolicyID).parseWithType(parse = ManagedIndexConfig.Companion::parse)
        }
    }

    private fun parserWithType(xc: String): XContentParser {
        return XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    }
}
