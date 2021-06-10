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

@file:Suppress("TopLevelPropertyNaming", "MatchingDeclarationName")
package org.opensearch.indexmanagement.indexstatemanagement.util

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import java.time.Instant

const val WITH_TYPE = "with_type"
val XCONTENT_WITHOUT_TYPE = ToXContent.MapParams(mapOf(WITH_TYPE to "false"))

const val FAILURES = "failures"
const val FAILED_INDICES = "failed_indices"
const val UPDATED_INDICES = "updated_indices"

const val ISM_TEMPLATE_FIELD = "policy.ism_template"

const val DEFAULT_PAGINATION_SIZE = 20
const val DEFAULT_PAGINATION_FROM = 0
const val DEFAULT_JOB_SORT_FIELD = "managed_index.index"
const val DEFAULT_POLICY_SORT_FIELD = "policy.policy_id.keyword"
const val DEFAULT_SORT_ORDER = "asc"
const val DEFAULT_QUERY_STRING = "*"

const val INDEX_HIDDEN = "index.hidden"
const val INDEX_NUMBER_OF_SHARDS = "index.number_of_shards"
const val INDEX_NUMBER_OF_REPLICAS = "index.number_of_replicas"

fun buildInvalidIndexResponse(builder: XContentBuilder, failedIndices: List<FailedIndex>) {
    if (failedIndices.isNotEmpty()) {
        builder.field(FAILURES, true)
        builder.startArray(FAILED_INDICES)
        for (failedIndex in failedIndices) {
            failedIndex.toXContent(builder, ToXContent.EMPTY_PARAMS)
        }
        builder.endArray()
    } else {
        builder.field(FAILURES, false)
        builder.startArray(FAILED_INDICES).endArray()
    }
}

data class FailedIndex(val name: String, val uuid: String, val reason: String) : Writeable, ToXContentFragment {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(INDEX_NAME_FIELD, name)
            .field(INDEX_UUID_FIELD, uuid)
            .field(REASON_FIELD, reason)
        return builder.endObject()
    }

    companion object {
        const val INDEX_NAME_FIELD = "index_name"
        const val INDEX_UUID_FIELD = "index_uuid"
        const val REASON_FIELD = "reason"
    }

    constructor(sin: StreamInput) : this(
        name = sin.readString(),
        uuid = sin.readString(),
        reason = sin.readString()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeString(uuid)
        out.writeString(reason)
    }
}

/**
 * Gets the XContentBuilder for partially updating a [ManagedIndexConfig]'s ChangePolicy
 */
fun getPartialChangePolicyBuilder(
    changePolicy: ChangePolicy?
): XContentBuilder {
    return XContentFactory.jsonBuilder()
        .startObject()
        .startObject(ManagedIndexConfig.MANAGED_INDEX_TYPE)
        .optionalTimeField(ManagedIndexConfig.LAST_UPDATED_TIME_FIELD, Instant.now())
        .field(ManagedIndexConfig.CHANGE_POLICY_FIELD, changePolicy)
        .endObject()
        .endObject()
}
