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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import java.io.IOException

data class ActionTimeout(val timeout: TimeValue) : ToXContentFragment, Writeable {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.field(TIMEOUT_FIELD, timeout.stringRep)
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        timeout = sin.readTimeValue()
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
            if (xcp.currentToken() == Token.VALUE_STRING) {
                return ActionTimeout(TimeValue.parseTimeValue(xcp.text(), TIMEOUT_FIELD))
            } else {
                throw IllegalArgumentException("Invalid token: [${xcp.currentToken()}] for ActionTimeout")
            }
        }
    }
}
