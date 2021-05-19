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

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
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
