/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

enum class SMState {
    START,
    CREATE_CONDITION_MET,
    DELETE_CONDITION_MET,
    CREATING,
    DELETING,
    FINISHED,
}
