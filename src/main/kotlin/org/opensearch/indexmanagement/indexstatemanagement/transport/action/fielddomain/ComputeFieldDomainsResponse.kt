/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.fielddomain

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.index.fielddomain.FieldDomain
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata

/** Encoded domain metadata only; privileged search responses and document contents never leave the calculator. */
class ComputeFieldDomainsResponse(customData: Map<String, String>) : ActionResponse() {
    private val customData: Map<String, String> = java.util.Map.copyOf(customData)

    constructor(sin: StreamInput) : this(sin.readMap(StreamInput::readString, StreamInput::readString))

    fun fieldDomains(): List<FieldDomain> = IndexFieldDomainMetadata.getInstance().validateAndParseCustomData(customData)

    override fun writeTo(out: StreamOutput) {
        out.writeMap(customData, StreamOutput::writeString, StreamOutput::writeString)
    }
}
