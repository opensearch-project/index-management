/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import io.kotest.property.Arb
import io.kotest.property.arbitrary.Codepoint
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.az
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.filter
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.string
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.test.OpenSearchTestCase

class RolloverConditionsValidationPropertyTests : OpenSearchTestCase() {

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

    fun `test mutual exclusivity of flat and grouped conditions is rejected`() = runBlocking {
        checkAll(100, Arb.int(0..3), Arb.long(1L..1000L)) { whichFlat, value ->
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

    fun `test empty any_of is rejected`() = runBlocking {
        checkAll(100, Arb.boolean()) { copyAlias ->
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

    fun `test empty group is rejected`() = runBlocking {
        checkAll(100, Arb.boolean()) { _ ->
            assertIaeContains("at least one condition") {
                RolloverConditionGroup(null, null, null, null)
            }
        }
    }

    fun `test non-positive group thresholds are rejected`() = runBlocking {
        checkAll(100, Arb.long(-100L..0L)) { nonPositiveDocs ->
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

    fun `test unknown field inside group is rejected`() = runBlocking {
        checkAll(100, Arb.string(1, 12, Codepoint.az()).filter { it !in KNOWN_FIELDS }) { unknownField ->
            val json = """
                { "$unknownField": "x" }
            """.trimIndent()
            val parser = org.opensearch.common.xcontent.XContentType.JSON.xContent().createParser(
                xContentRegistry(),
                org.opensearch.common.xcontent.LoggingDeprecationHandler.INSTANCE,
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
