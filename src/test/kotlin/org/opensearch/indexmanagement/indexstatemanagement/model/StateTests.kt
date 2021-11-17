/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.indexmanagement.indexstatemanagement.randomDeleteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReplicaCountActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.randomTransition
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class StateTests : OpenSearchTestCase() {

    fun `test invalid state name`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for blank state name") {
            State(" ", emptyList(), emptyList())
        }

        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for empty state name") {
            State("", emptyList(), emptyList())
        }
    }

    fun `test transitions disallowed if using delete`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for transitions when using delete") {
            randomState(actions = listOf(randomDeleteActionConfig()), transitions = listOf(randomTransition()))
        }
    }

    fun `test action disallowed if used after delete`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for action if used after delete") {
            randomState(actions = listOf(randomDeleteActionConfig(), randomReplicaCountActionConfig()))
        }
    }
}
