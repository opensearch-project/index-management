/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:Suppress("TopLevelPropertyNaming", "MatchingDeclarationName")
package org.opensearch.indexmanagement.snapshotmanagement.util

import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.NAME_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_TYPE
import org.opensearch.rest.RestRequest

const val SM_POLICY_NAME_KEYWORD = "$SM_TYPE.$NAME_FIELD"
const val DEFAULT_SM_POLICY_SORT_FIELD = SM_POLICY_NAME_KEYWORD

fun RestRequest.getNonEmptySMPolicyName(): String {
    val policyName = this.param("policyName", "")
    if (policyName.isEmpty()) {
        throw IllegalArgumentException("Missing policy name")
    }
    return policyName
}
