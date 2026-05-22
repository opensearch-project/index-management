/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.refreshanalyzer

import org.opensearch.Version
import org.opensearch.action.support.broadcast.BroadcastRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

class RefreshSearchAnalyzerRequest : BroadcastRequest<RefreshSearchAnalyzerRequest> {
    val reloadCachedResources: Boolean

    @Suppress("SpreadOperator")
    constructor(vararg indices: String, reloadCachedResources: Boolean = false) : super(*indices) {
        this.reloadCachedResources = reloadCachedResources
    }

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp) {
        reloadCachedResources = if (inp.version.onOrAfter(Version.V_3_7_0)) {
            inp.readBoolean()
        } else {
            false
        }
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        if (out.version.onOrAfter(Version.V_3_7_0)) {
            out.writeBoolean(reloadCachedResources)
        }
    }
}
