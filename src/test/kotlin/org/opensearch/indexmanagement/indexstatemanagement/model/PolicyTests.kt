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

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
