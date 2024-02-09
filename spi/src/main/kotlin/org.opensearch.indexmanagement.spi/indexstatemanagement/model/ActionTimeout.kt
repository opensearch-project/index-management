/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.model

import org.opensearch.common.unit.TimeValue
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentFragment
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import java.io.IOException

data class ActionTimeout(val timeout: TimeValue) : ToXContentFragment, Writeable {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.field(TIMEOUT_FIELD, timeout.stringRep)
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        timeout = sin.readTimeValue(),
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeTimeValue(timeout)
    }

    companion object {
        const val TIMEOUT_FIELD = "timeout"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionTimeout {
            if (xcp.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ActionTimeout(TimeValue.parseTimeValue(xcp.text(), TIMEOUT_FIELD))
            } else {
                throw IllegalArgumentException("Invalid token: [${xcp.currentToken()}] for ActionTimeout")
            }
        }
    }
}
