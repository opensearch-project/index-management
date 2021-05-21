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

package org.opensearch.indexmanagement.transform.model

import org.opensearch.action.index.IndexRequest

data class TransformSearchResult(val stats: TransformStats, val docsToIndex: List<IndexRequest>, val afterKey: Map<String, Any>? = null)
