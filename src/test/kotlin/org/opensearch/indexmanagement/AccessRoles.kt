/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import org.opensearch.indexmanagement.rollup.action.get.GetRollupAction
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupAction

const val WRITE_POLICY = IndexPolicyAction.NAME
const val GET_POLICIES = GetPoliciesAction.NAME
const val GET_POLICY = GetPolicyAction.NAME
const val EXPLAIN_INDEX = ExplainAction.NAME
const val MANAGED_INDEX = ManagedIndexAction.NAME
const val ROLLUP_ALL = "cluster:admin/opendistro/rollup/*"
const val INDEX_ROLLUP = IndexRollupAction.NAME
const val GET_ROLLUP = GetRollupAction.NAME
const val EXPLAIN_ROLLUP = ExplainRollupAction.NAME

// Index related
const val GET_INDEX = "indices:admin/mappings/get"
const val SEARCH_INDEX = "indices:data/read/search"
