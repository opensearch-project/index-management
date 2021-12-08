/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ReplicaCountActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        TODO("Not yet implemented")
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        TODO("Not yet implemented")
    }

    override fun getActionType(): String {
        TODO("Not yet implemented")
    }
}
