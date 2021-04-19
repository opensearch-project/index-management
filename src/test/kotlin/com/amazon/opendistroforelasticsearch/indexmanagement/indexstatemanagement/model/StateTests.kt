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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomDeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomState
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomTransition
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
