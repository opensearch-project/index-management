/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.indexmanagement.transform.randomTransformMetadata
import org.opensearch.indexmanagement.transform.toJsonString
import org.opensearch.search.SearchModule
import org.opensearch.test.OpenSearchTestCase

class XContentTests : OpenSearchTestCase() {

    fun `test transform metadata parsing without type`() {
        val transformMetadata = randomTransformMetadata()
        val transformMetadataString = transformMetadata.toJsonString(XCONTENT_WITHOUT_TYPE)
        val parsedTransformMetadata = TransformMetadata.parse(
            parser(transformMetadataString), transformMetadata.id, transformMetadata.seqNo, transformMetadata.primaryTerm
        )
        assertEquals("Round tripping Transform metadata without type doesn't work", transformMetadata, parsedTransformMetadata)
    }

    fun `test transform metadata parsing with type`() {
        val transformMetadata = randomTransformMetadata()
        val transformMetadataString = transformMetadata.toJsonString()
        val parser = parserWithType(transformMetadataString)
        val parsedTransformMetadata = parser.parseWithType(
            transformMetadata.id, transformMetadata.seqNo, transformMetadata.primaryTerm, TransformMetadata.Companion::parse
        )
        assertEquals("Round tripping Transform metadata with type doesn't work", transformMetadata, parsedTransformMetadata)
    }

    fun `test transform parsing without type`() {
        val transform = randomTransform()
        val transformString = transform.toJsonString(XCONTENT_WITHOUT_TYPE)
        val parsedTransform = Transform.parse(parser(transformString), transform.id, transform.seqNo, transform.primaryTerm)
        // roles are deprecated and not populated in toXContent and parsed as part of parse
        assertEquals("Round tripping Transform without type doesn't work", transform.copy(roles = listOf()), parsedTransform)
    }

    fun `test transform parsing with type`() {
        val transform = randomTransform()
        val transformString = transform.toJsonString()
        val parser = parserWithType(transformString)
        val parsedTransform = parser.parseWithType(transform.id, transform.seqNo, transform.primaryTerm, Transform.Companion::parse)
        // roles are deprecated and not populated in toXContent and parsed as part of parse
        assertEquals("Round tripping Transform with type doesn't work", transform.copy(roles = listOf()), parsedTransform)
    }

    fun `test transform parsing should ignore metadata id and startTime if its newly created transform`() {
        val transform = randomTransform().copy(metadataId = "randomMetadata")
        val transformString = transform.toJsonString(XCONTENT_WITHOUT_TYPE)
        val parsedTransform = Transform.parse(parser(transformString), transform.id)
        assertNull("MetadataId is not removed when parsing the transform during creation", parsedTransform.metadataId)
    }

    private fun parser(xc: String): XContentParser {
        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
        parser.nextToken()
        return parser
    }

    private fun parserWithType(xc: String): XContentParser {
        return XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    }

    override fun xContentRegistry(): NamedXContentRegistry {
        return NamedXContentRegistry(SearchModule(Settings.EMPTY, emptyList()).namedXContents)
    }
}
