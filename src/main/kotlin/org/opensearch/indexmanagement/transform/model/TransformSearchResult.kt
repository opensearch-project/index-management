/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.action.index.IndexRequest

data class TransformSearchResult(val stats: TransformStats, val docsToIndex: List<IndexRequest>, val afterKey: Map<String, Any>? = null)
