/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import org.junit.Assert
import org.opensearch.test.OpenSearchTestCase

class TargetIndexMappingServiceTests : OpenSearchTestCase() {

    fun `test create target index mapping fields mapped correctly`() {
        val expectedResult = """{"_meta":{"schema_version":1},"dynamic_templates":[{"strings":{"match_mapping_type":"string","mapping":{"type":"keyword"}}}],"properties":{"tpep_pickup_datetime":{"type":"date"}}}"""
        val dateFieldMap = mapOf("tpep_pickup_datetime" to mapOf("type" to "date"))
        val result = TargetIndexMappingService.createTargetIndexMapping(dateFieldMap)
        Assert.assertNotNull(result)
        assertEquals("Target index mapping with date fields not correct", expectedResult.trimIndent(), result.trimIndent())
    }

    fun `test create target index mapping empty map`() {
        val expectedResult = """{"_meta":{"schema_version":1},"dynamic_templates":[{"strings":{"match_mapping_type":"string","mapping":{"type":"keyword"}}}]}"""
        val result = TargetIndexMappingService.createTargetIndexMapping(emptyMap())
        Assert.assertNotNull(result)
        assertEquals("Target index mapping with date fields not correct", expectedResult.trimIndent(), result.trimIndent())
    }
}
