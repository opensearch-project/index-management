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

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions.addValidationError
import org.opensearch.action.support.master.AcknowledgedRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.index.Index
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData

class UpdateManagedIndexMetaDataRequest : AcknowledgedRequest<UpdateManagedIndexMetaDataRequest> {

    var indicesToAddManagedIndexMetaDataTo: List<Pair<Index, ManagedIndexMetaData>>
        private set

    var indicesToRemoveManagedIndexMetaDataFrom: List<Index>
        private set

    constructor(si: StreamInput) : super(si) {
        indicesToAddManagedIndexMetaDataTo = si.readList {
            val index = Index(it)
            val managedIndexMetaData = ManagedIndexMetaData.fromStreamInput(it)
            Pair(index, managedIndexMetaData)
        }

        indicesToRemoveManagedIndexMetaDataFrom = si.readList { Index(it) }
    }

    constructor(
        indicesToAddManagedIndexMetaDataTo: List<Pair<Index, ManagedIndexMetaData>> = listOf(),
        indicesToRemoveManagedIndexMetaDataFrom: List<Index> = listOf()
    ) {
        this.indicesToAddManagedIndexMetaDataTo = indicesToAddManagedIndexMetaDataTo
        this.indicesToRemoveManagedIndexMetaDataFrom = indicesToRemoveManagedIndexMetaDataFrom
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null

        if (this.indicesToAddManagedIndexMetaDataTo.isEmpty() && this.indicesToRemoveManagedIndexMetaDataFrom.isEmpty()) {
            validationException = addValidationError(
                "At least one non-empty List must be given for UpdateManagedIndexMetaDataRequest",
                validationException
            )
        }

        return validationException
    }

    override fun writeTo(streamOutput: StreamOutput) {
        super.writeTo(streamOutput)

        streamOutput.writeCollection(indicesToAddManagedIndexMetaDataTo) { so, pair ->
            pair.first.writeTo(so)
            pair.second.writeTo(so)
        }

        streamOutput.writeCollection(indicesToRemoveManagedIndexMetaDataFrom) { so, index ->
            index.writeTo(so)
        }
    }
}
