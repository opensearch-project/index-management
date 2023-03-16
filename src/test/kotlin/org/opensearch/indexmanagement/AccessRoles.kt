/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.opensearch.indexmanagement.adminpanel.notification.action.delete.DeleteLRONConfigAction
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigAction
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigsAction
import org.opensearch.indexmanagement.adminpanel.notification.action.index.IndexLRONConfigAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.opensearch.indexmanagement.rollup.action.delete.DeleteRollupAction
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import org.opensearch.indexmanagement.rollup.action.get.GetRollupAction
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupAction
import org.opensearch.indexmanagement.rollup.action.mapping.UpdateRollupMappingAction
import org.opensearch.indexmanagement.transform.action.delete.DeleteTransformsAction
import org.opensearch.indexmanagement.transform.action.explain.ExplainTransformAction
import org.opensearch.indexmanagement.transform.action.get.GetTransformAction
import org.opensearch.indexmanagement.transform.action.get.GetTransformsAction
import org.opensearch.indexmanagement.transform.action.index.IndexTransformAction
import org.opensearch.indexmanagement.transform.action.start.StartTransformAction
import org.opensearch.indexmanagement.transform.action.stop.StopTransformAction

// ISM
const val WRITE_POLICY = IndexPolicyAction.NAME
const val ADD_POLICY = AddPolicyAction.NAME
const val GET_POLICIES = GetPoliciesAction.NAME
const val GET_POLICY = GetPolicyAction.NAME
const val EXPLAIN_INDEX = ExplainAction.NAME
const val MANAGED_INDEX = ManagedIndexAction.NAME
const val DELETE_POLICY = DeletePolicyAction.NAME
// Rollup
const val ROLLUP_ALL = "cluster:admin/opendistro/rollup/*"
const val INDEX_ROLLUP = IndexRollupAction.NAME
const val GET_ROLLUP = GetRollupAction.NAME
const val EXPLAIN_ROLLUP = ExplainRollupAction.NAME
const val UPDATE_ROLLUP = UpdateRollupMappingAction.NAME
const val DELETE_ROLLUP = DeleteRollupAction.NAME
// Transform
const val TRANSFORM_ACTION = IndexTransformAction.NAME
const val GET_TRANSFORM = GetTransformAction.NAME
const val EXPLAIN_TRANSFORM = ExplainTransformAction.NAME
const val START_TRANSFORM = StartTransformAction.NAME
const val DELETE_TRANSFORM = DeleteTransformsAction.NAME
const val GET_TRANSFORMS = GetTransformsAction.NAME
const val STOP_TRANSFORM = StopTransformAction.NAME
// In order to execute transform, user need to have health privilege
const val HEALTH = "cluster:monitor/health"
// Index
const val GET_INDEX_MAPPING = "indices:admin/mappings/get"
const val PUT_INDEX_MAPPING = "indices:admin/mapping/put"
const val SEARCH_INDEX = "indices:data/read/search"
const val CREATE_INDEX = "indices:admin/create"
const val WRITE_INDEX = "indices:data/write/index"
const val BULK_WRITE_INDEX = "indices:data/write/bulk*"
// Long-running operation notification (lron)
const val INDEX_LRON_CONFIG = IndexLRONConfigAction.NAME
const val GET_LRON_CONFIG = GetLRONConfigAction.NAME
const val GET_LRON_CONFIGS = GetLRONConfigsAction.NAME
const val DELETE_LRON_CONFIG = DeleteLRONConfigAction.NAME
