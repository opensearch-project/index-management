/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.test.OpenSearchTestCase

class RolloverConditionsValidationPropertyTests : OpenSearchTestCase() {

    private val iterations = 100

    private fun assertIaeContains(vararg needles: String, block: () -> Unit) {
        try {
            block()
            fail("Expected IllegalArgumentException but none was thrown")
        } catch (e: IllegalArgumentException) {
            val message = e.message ?: ""
            needles.forEach {
                assertTrue("Error message [$message] should contain [$it]", message.contains(it))
            }
        }
    }

    private fun randomUnknownFieldName(): String {
        while (true) {
            val candidate = randomAlphaOfLength(randomIntBetween(1, 12)).lowercase()
            if (candidate.isNotEmpty() && candidate !in KNOWN_FIELDS) return candidate
        }
    }

    fun `test mutual exclusivity of flat and grouped conditions is rejected`() {
        repeat(iterations) {
            val whichFlat = randomIntBetween(0, 3)
            val value = randomLongBetween(1L, 1000L)
            val minSize = if (whichFlat == 0) ByteSizeValue(value) else null
            val minDocs = if (whichFlat == 1) value else null
            val minAge = if (whichFlat == 2) TimeValue.timeValueMillis(value) else null
            val minPrimaryShardSize = if (whichFlat == 3) ByteSizeValue(value) else null
            val group = RolloverConditionGroup(null, value, null, null)

            assertIaeContains(RolloverAction.ANY_OF_FIELD) {
                RolloverAction(
                    minSize = minSize, minDocs = minDocs, minAge = minAge, minPrimaryShardSize = minPrimaryShardSize,
                    conditionGroups = listOf(group),
                    index = 0,
                )
            }
        }
    }

    fun `test empty any_of is rejected`() {
        repeat(iterations) {
            val copyAlias = randomBoolean()
            assertIaeContains(RolloverAction.ANY_OF_FIELD) {
                RolloverAction(
                    minSize = null, minDocs = null, minAge = null, minPrimaryShardSize = null,
                    copyAlias = copyAlias,
                    conditionGroups = emptyList(),
                    index = 0,
                )
            }
        }
    }

    fun `test empty group is rejected`() {
        repeat(iterations) {
            assertIaeContains("at least one condition") {
                RolloverConditionGroup(null, null, null, null)
            }
        }
    }

    fun `test non-positive group thresholds are rejected`() {
        repeat(iterations) {
            val nonPositiveDocs = randomLongBetween(-100L, 0L)
            assertIaeContains("greater than 0") {
                RolloverConditionGroup(null, nonPositiveDocs, null, null)
            }
            assertIaeContains("greater than 0") {
                RolloverConditionGroup(ByteSizeValue(0), null, null, null)
            }
            assertIaeContains("greater than 0") {
                RolloverConditionGroup(null, null, null, ByteSizeValue(0))
            }
        }
    }

    fun `test unknown field inside group is rejected`() {
        repeat(iterations) {
            val unknownField = randomUnknownFieldName()
            val json = """
                { "$unknownField": "x" }
            """.trimIndent()
            val parser = XContentType.JSON.xContent().createParser(
                xContentRegistry(),
                LoggingDeprecationHandler.INSTANCE,
                json,
            )
            parser.nextToken()
            assertIaeContains(unknownField) {
                RolloverConditionGroup.parse(parser)
            }
        }
    }

    companion object {
        private val KNOWN_FIELDS = setOf(
            RolloverAction.MIN_SIZE_FIELD,
            RolloverAction.MIN_DOC_COUNT_FIELD,
            RolloverAction.MIN_INDEX_AGE_FIELD,
            RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD,
        )
    }
}
