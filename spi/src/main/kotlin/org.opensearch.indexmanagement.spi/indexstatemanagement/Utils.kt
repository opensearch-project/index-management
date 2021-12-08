/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement

import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder

// forIndex means saving to config index, distinguish from Explain and History,
// which only show meaningful partial metadata
@Suppress("ReturnCount")
fun XContentBuilder.addObject(name: String, metadata: ToXContentFragment?, params: ToXContent.Params, forIndex: Boolean = false): XContentBuilder {
    if (metadata != null) return this.buildMetadata(name, metadata, params)
    return if (forIndex) nullField(name) else this
}

fun XContentBuilder.buildMetadata(name: String, metadata: ToXContentFragment, params: ToXContent.Params): XContentBuilder {
    this.startObject(name)
    metadata.toXContent(this, params)
    this.endObject()
    return this
}
