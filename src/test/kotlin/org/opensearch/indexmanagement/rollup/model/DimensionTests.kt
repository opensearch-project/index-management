/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.opensearch.indexmanagement.rollup.randomDateHistogram
import org.opensearch.indexmanagement.rollup.randomHistogram
import org.opensearch.indexmanagement.rollup.randomTerms
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class DimensionTests : OpenSearchTestCase() {

    fun `test date histogram empty fields`() {
        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomDateHistogram().copy(sourceField = "", targetField = "")
        }

        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomDateHistogram().copy(sourceField = "source", targetField = "")
        }

        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomDateHistogram().copy(sourceField = "", targetField = "target")
        }
    }

    fun `test precisely one date histogram interval`() {
        assertFailsWith(IllegalArgumentException::class, "Must specify a fixed or calendar interval") {
            randomDateHistogram().copy(fixedInterval = null, calendarInterval = null)
        }

        assertFailsWith(IllegalArgumentException::class, "Can only specify a fixed or calendar interval") {
            randomDateHistogram().copy(fixedInterval = "30m", calendarInterval = "1d")
        }
    }

    fun `test histogram empty fields`() {
        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomHistogram().copy(sourceField = "", targetField = "")
        }

        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomHistogram().copy(sourceField = "source", targetField = "")
        }

        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomHistogram().copy(sourceField = "", targetField = "target")
        }
    }

    fun `test histogram interval must be positive decimal`() {
        assertFailsWith(IllegalArgumentException::class, "Interval must be a positive decimal") {
            randomHistogram().copy(interval = 0.0)
        }

        assertFailsWith(IllegalArgumentException::class, "Interval must be a positive decimal") {
            randomHistogram().copy(interval = -1.3)
        }
    }

    fun `test terms empty fields`() {
        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomTerms().copy(sourceField = "", targetField = "")
        }

        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomTerms().copy(sourceField = "source", targetField = "")
        }

        assertFailsWith(IllegalArgumentException::class, "Source and target field must not be empty") {
            randomTerms().copy(sourceField = "", targetField = "target")
        }
    }
}
