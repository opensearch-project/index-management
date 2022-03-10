/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement

import org.opensearch.cluster.ClusterState

interface StatusChecker {

    /**
     * checks and returns the status of the extension
     */
    fun check(clusterState: ClusterState): Status {
        return Status.ENABLED
    }
}

enum class Status(private val value: String) {
    ENABLED("enabled"),
    DISABLED("disabled");

    override fun toString(): String {
        return value
    }
}

class DefaultStatusChecker : StatusChecker
