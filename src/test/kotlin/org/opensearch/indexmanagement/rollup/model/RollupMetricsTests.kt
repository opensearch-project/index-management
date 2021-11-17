/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.opensearch.indexmanagement.rollup.randomAverage
import org.opensearch.indexmanagement.rollup.randomRollupMetrics
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
