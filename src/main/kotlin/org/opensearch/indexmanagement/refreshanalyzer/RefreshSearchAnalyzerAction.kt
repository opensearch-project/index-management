/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.refreshanalyzer

import org.opensearch.action.ActionType
import org.opensearch.common.io.stream.Writeable

class RefreshSearchAnalyzerAction : ActionType<RefreshSearchAnalyzerResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/refresh_search_analyzers"
        val INSTANCE = RefreshSearchAnalyzerAction()
        val reader = Writeable.Reader { inp -> RefreshSearchAnalyzerResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<RefreshSearchAnalyzerResponse> = reader
}
