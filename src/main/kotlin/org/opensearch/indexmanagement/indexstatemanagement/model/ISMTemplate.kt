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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.apache.logging.log4j.LogManager
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.opensearchapi.instant
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import java.io.IOException
import java.lang.IllegalArgumentException
import java.time.Instant

private val log = LogManager.getLogger(ISMTemplate::class.java)

data class ISMTemplate(
    val indexPatterns: List<String>,
    val priority: Int,
    val lastUpdatedTime: Instant
) : ToXContentObject, Writeable {

    init {
        require(priority >= 0) { "Requires priority to be >= 0" }
        require(indexPatterns.isNotEmpty()) { "Requires at least one index pattern" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(INDEX_PATTERN, indexPatterns)
            .field(PRIORITY, priority)
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, lastUpdatedTime)
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readStringList(),
        sin.readInt(),
        sin.readInstant()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indexPatterns)
        out.writeInt(priority)
        out.writeInstant(lastUpdatedTime)
    }

    companion object {
        const val ISM_TEMPLATE_TYPE = "ism_template"
        const val INDEX_PATTERN = "index_patterns"
        const val PRIORITY = "priority"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"

        @Suppress("ComplexMethod")
        fun parse(xcp: XContentParser): ISMTemplate {
            val indexPatterns: MutableList<String> = mutableListOf()
            var priority = 0
            var lastUpdatedTime: Instant? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    INDEX_PATTERN -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            indexPatterns.add(xcp.text())
                        }
                    }
                    PRIORITY -> priority = if (xcp.currentToken() == Token.VALUE_NULL) 0 else xcp.intValue()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = xcp.instant()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ISMTemplate.")
                }
            }

            return ISMTemplate(
                indexPatterns = indexPatterns,
                priority = priority,
                lastUpdatedTime = lastUpdatedTime ?: Instant.now()
            )
        }
    }
}
