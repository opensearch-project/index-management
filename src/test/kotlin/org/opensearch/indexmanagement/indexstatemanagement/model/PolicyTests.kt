/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.randomTransition
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class PolicyTests : OpenSearchTestCase() {

    fun `test invalid default state`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for invalid default state") {
            randomPolicy().copy(defaultState = "definitely not this")
        }
    }

    fun `test empty states`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for empty states") {
            randomPolicy().copy(states = emptyList())
        }
    }

    fun `test duplicate states`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for duplicate states") {
            val states = listOf(randomState(name = "duplicate"), randomState(), randomState(name = "duplicate"))
            randomPolicy(states = states)
        }
    }

    fun `test transition pointing to nonexistent state`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for nonexistent transition state") {
            val states = listOf(randomState(transitions = listOf(randomTransition(stateName = "doesnt exist"))), randomState(), randomState())
            randomPolicy(states = states)
        }
    }
}
