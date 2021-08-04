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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.settings.LegacyOpenDistroManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_USER
import java.io.IOException

open class ExplainResponse : ActionResponse, ToXContentObject {

    // TODO refactor these lists usage to map
    val indexNames: List<String>
    val indexPolicyIDs: List<String?>
    val indexMetadatas: List<ManagedIndexMetaData?>

    constructor(
        indexNames: List<String>,
        indexPolicyIDs: List<String?>,
        indexMetadatas: List<ManagedIndexMetaData?>
    ) : super() {
        this.indexNames = indexNames
        this.indexPolicyIDs = indexPolicyIDs
        this.indexMetadatas = indexMetadatas
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indexNames = sin.readStringList(),
        indexPolicyIDs = sin.readStringList(),
        indexMetadatas = sin.readList { ManagedIndexMetaData.fromStreamInput(it) }
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indexNames)
        out.writeStringCollection(indexPolicyIDs)
        out.writeCollection(indexMetadatas)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        indexNames.forEachIndexed { ind, name ->
            builder.startObject(name)
            builder.field(LegacyOpenDistroManagedIndexSettings.POLICY_ID.key, indexPolicyIDs[ind])
            builder.field(ManagedIndexSettings.POLICY_ID.key, indexPolicyIDs[ind])
            indexMetadatas[ind]?.toXContent(builder, XCONTENT_WITHOUT_USER)
            builder.endObject()
        }
        return builder.endObject()
    }
}
