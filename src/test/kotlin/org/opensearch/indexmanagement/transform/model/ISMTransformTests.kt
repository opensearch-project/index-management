/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.indexmanagement.transform.randomISMTransform
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class ISMTransformTests : OpenSearchTestCase() {
    fun `test ism transform requires non empty description`() {
        assertFailsWith(IllegalArgumentException::class, "Requires non empty description") {
            randomISMTransform().copy(description = "")
        }
    }

    fun `test ism transform requires non empty target index`() {
        assertFailsWith(IllegalArgumentException::class, "Requires non empty targetIndex") {
            randomISMTransform().copy(targetIndex = "")
        }
    }

    fun `test ism transform requires non empty groups`() {
        assertFailsWith(IllegalArgumentException::class, "Requires non empty groups") {
            randomISMTransform().copy(groups = listOf())
        }
    }

    fun `test ism transform requires page size between 1 and 10K`() {
        assertFailsWith(IllegalArgumentException::class, "Page size cannot be less than 1") {
            randomISMTransform().copy(pageSize = -1)
        }

        assertFailsWith(IllegalArgumentException::class, "Page size cannot be less than 1") {
            randomISMTransform().copy(pageSize = 0)
        }

        assertFailsWith(IllegalArgumentException::class, "Page size cannot be greater than 10000") {
            randomISMTransform().copy(pageSize = 10001)
        }

        randomISMTransform().copy(pageSize = 1)
        randomISMTransform().copy(pageSize = 500)
        randomISMTransform().copy(pageSize = 10000)
    }
}
