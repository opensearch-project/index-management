/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.toJsonString
import org.opensearch.test.OpenSearchTestCase

class XContentTests : OpenSearchTestCase() {

    fun `test sm policy parsing`() {
        val smPolicy = randomSMPolicy()
        val smPolicyString = smPolicy.toJsonString()
        val parser = parserWithType(smPolicyString)
        val parsedSMPolicy = parser.parseWithType(smPolicy.id, smPolicy.seqNo, smPolicy.primaryTerm, SMPolicy.Companion::parse)
        assertEquals("Round tripping sm policy with type doesn't work", smPolicy, parsedSMPolicy)
    }

    fun `test sm policy parsing without type`() {
        val smPolicy = randomSMPolicy()
        val smPolicyString = smPolicy.toJsonString()
        val parsedSMPolicy = SMPolicy.parse(smPolicyString.parser(), smPolicy.id, smPolicy.seqNo, smPolicy.primaryTerm)
        assertEquals("Round tripping sm policy without type doesn't work", smPolicy, parsedSMPolicy)
    }

    fun `test sm metadata parsing`() {
        val smMetadata = randomSMMetadata()
        val smMetadataString = smMetadata.toJsonString()
        val parsedSMMetadata = smMetadataString.parser().parseWithType(parse = SMMetadata.Companion::parse)
        assertEquals("Round tripping sm metadata doesn't work", smMetadata, parsedSMMetadata)
    }

    private fun parserWithType(xc: String): XContentParser {
        return XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    }

    private fun String.parser(): XContentParser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, this)
}
