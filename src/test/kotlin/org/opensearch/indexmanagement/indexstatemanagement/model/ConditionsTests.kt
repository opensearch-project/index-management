/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.randomByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.randomTimeValueObject
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class ConditionsTests : OpenSearchTestCase() {

    fun `test supplying more than one transition condition fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for supplying multiple transition conditions"
        ) {
            Conditions(indexAge = randomTimeValueObject(), size = randomByteSizeValue())
        }
    }

    fun `test doc count condition of zero fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for doc count condition less than 1"
        ) {
            Conditions(docCount = 0)
        }
    }

    fun `test size condition of zero fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for size condition less than 1"
        ) {
            Conditions(size = ByteSizeValue.parseBytesSizeValue("0", "size_test"))
        }
    }
}
