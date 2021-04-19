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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomAverage
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollupMetrics
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.rest.OpenSearchRestTestCase
import kotlin.test.assertFailsWith

class RollupMetricsTests : OpenSearchTestCase() {
    fun `test rollup metrics empty fields`() {
        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomRollupMetrics().copy(sourceField = "", targetField = "")
        }

        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomRollupMetrics().copy(sourceField = "source", targetField = "")
        }

        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomRollupMetrics().copy(sourceField = "", targetField = "target")
        }
    }

    fun `test rollup metrics needs at least one metric`() {
        val field = OpenSearchRestTestCase.randomAlphaOfLength(10)
        assertFailsWith(IllegalArgumentException::class, "Must specify at least one metric to aggregate on for $field") {
            randomRollupMetrics().copy(sourceField = field, targetField = field, metrics = emptyList())
        }
    }

    fun `test rollup metrics distinct metrics`() {
        assertFailsWith(IllegalArgumentException::class, "Cannot have multiple metrics of the same type in a single rollup metric") {
            randomRollupMetrics().copy(metrics = listOf(randomAverage(), randomAverage()))
        }
    }
}
