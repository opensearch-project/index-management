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

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import java.io.IOException

class GetPoliciesResponse : ActionResponse, ToXContentObject {

    val policies: List<Policy>
    val totalPolicies: Int

    constructor(
        policies: List<Policy>,
        totalPolicies: Int
    ) : super() {
        this.policies = policies
        this.totalPolicies = totalPolicies
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policies = sin.readList(::Policy),
        totalPolicies = sin.readInt()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeCollection(policies)
        out.writeInt(totalPolicies)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startArray("policies")
            .apply {
                for (policy in policies) {
                    this.startObject()
                        .field(_ID, policy.id)
                        .field(_SEQ_NO, policy.seqNo)
                        .field(_PRIMARY_TERM, policy.primaryTerm)
                        .field(Policy.POLICY_TYPE, policy, XCONTENT_WITHOUT_TYPE_AND_USER)
                        .endObject()
                }
            }
            .endArray()
            .field("total_policies", totalPolicies)
            .endObject()
    }
}
