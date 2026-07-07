/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.util

import org.opensearch.common.unit.TimeValue
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverConditionGroup
import org.opensearch.test.OpenSearchTestCase

class RolloverConditionsEvaluationPropertyTests : OpenSearchTestCase() {

    private val iterations = 100

    private data class IndexState(
        val indexAge: TimeValue,
        val numDocs: Long,
        val indexSize: ByteSizeValue,
        val primaryShardSize: ByteSizeValue,
    )

    private data class ConditionSpec(
        val minSize: ByteSizeValue?,
        val minDocs: Long?,
        val minAge: TimeValue?,
        val minPrimaryShardSize: ByteSizeValue?,
    ) {
        fun isEmpty(): Boolean = minSize == null && minDocs == null && minAge == null && minPrimaryShardSize == null
    }

    private fun randomIndexState(): IndexState = IndexState(
        indexAge = TimeValue.timeValueMillis(randomLongBetween(0L, 100L)),
        numDocs = randomLongBetween(0L, 100L),
        indexSize = ByteSizeValue(randomLongBetween(0L, 100L)),
        primaryShardSize = ByteSizeValue(randomLongBetween(0L, 100L)),
    )

    private fun randomFlatSpec(): ConditionSpec = ConditionSpec(
        minSize = if (randomBoolean()) ByteSizeValue(randomLongBetween(1L, 100L)) else null,
        minDocs = if (randomBoolean()) randomLongBetween(1L, 100L) else null,
        minAge = if (randomBoolean()) TimeValue.timeValueMillis(randomLongBetween(1L, 100L)) else null,
        minPrimaryShardSize = if (randomBoolean()) ByteSizeValue(randomLongBetween(1L, 100L)) else null,
    )

    private fun randomGroupSpec(): ConditionSpec {
        while (true) {
            val spec = randomFlatSpec()
            if (!spec.isEmpty()) return spec
        }
    }

    private fun randomGroupSpecList(): List<ConditionSpec> = (1..randomIntBetween(1, 4)).map { randomGroupSpec() }

    private fun randomSingleConditionSpec(): ConditionSpec = when (randomIntBetween(0, 3)) {
        0 -> ConditionSpec(ByteSizeValue(randomLongBetween(1L, 100L)), null, null, null)
        1 -> ConditionSpec(null, randomLongBetween(1L, 100L), null, null)
        2 -> ConditionSpec(null, null, TimeValue.timeValueMillis(randomLongBetween(1L, 100L)), null)
        else -> ConditionSpec(null, null, null, ByteSizeValue(randomLongBetween(1L, 100L)))
    }

    private fun results(spec: ConditionSpec, state: IndexState): List<Boolean> = buildList {
        if (spec.minDocs != null) add(spec.minDocs <= state.numDocs)
        if (spec.minAge != null) add(spec.minAge.millis <= state.indexAge.millis)
        if (spec.minSize != null) add(spec.minSize <= state.indexSize)
        if (spec.minPrimaryShardSize != null) add(spec.minPrimaryShardSize <= state.primaryShardSize)
    }

    private fun flatAction(spec: ConditionSpec): RolloverAction = RolloverAction(
        minSize = spec.minSize,
        minDocs = spec.minDocs,
        minAge = spec.minAge,
        minPrimaryShardSize = spec.minPrimaryShardSize,
        preventEmptyRollover = false,
        index = 0,
    )

    private fun groupedAction(specs: List<ConditionSpec>): RolloverAction = RolloverAction(
        minSize = null,
        minDocs = null,
        minAge = null,
        minPrimaryShardSize = null,
        preventEmptyRollover = false,
        conditionGroups = specs.map { RolloverConditionGroup(it.minSize, it.minDocs, it.minAge, it.minPrimaryShardSize) },
        index = 0,
    )

    private fun RolloverAction.evaluate(state: IndexState): Boolean =
        evaluateConditions(state.indexAge, state.numDocs, state.indexSize, state.primaryShardSize)

    fun `test grouped evaluation matches OR-of-ANDs reference`() {
        repeat(iterations) {
            val specs = randomGroupSpecList()
            val state = randomIndexState()
            val expected = specs.any { spec -> results(spec, state).all { it } }
            assertEquals(
                "Grouped evaluation must equal OR-of-ANDs reference",
                expected,
                groupedAction(specs).evaluate(state),
            )
        }
    }

    fun `test flat evaluation matches any-condition-satisfied reference`() {
        repeat(iterations) {
            val spec = randomFlatSpec()
            val state = randomIndexState()
            val r = results(spec, state)
            val expected = if (r.isEmpty()) true else r.any { it }
            assertEquals(
                "Flat evaluation must equal the OR reference",
                expected,
                flatAction(spec).evaluate(state),
            )
        }
    }

    fun `test single-condition group equals single flat condition`() {
        repeat(iterations) {
            val spec = randomSingleConditionSpec()
            val state = randomIndexState()
            assertEquals(
                "A one-condition group must match the equivalent flat condition",
                flatAction(spec).evaluate(state),
                groupedAction(listOf(spec)).evaluate(state),
            )
        }
    }

    fun `test single group equals AND of its conditions`() {
        repeat(iterations) {
            val spec = randomGroupSpec()
            val state = randomIndexState()
            val expected = results(spec, state).all { it }
            assertEquals(
                "A single group must be satisfied only when all its conditions hold",
                expected,
                groupedAction(listOf(spec)).evaluate(state),
            )
        }
    }

    fun `test no conditions at all evaluates true`() {
        repeat(iterations) {
            val state = randomIndexState()
            val action = RolloverAction(
                minSize = null, minDocs = null, minAge = null, minPrimaryShardSize = null,
                preventEmptyRollover = false, index = 0,
            )
            assertTrue("An action with no conditions must always evaluate true", action.evaluate(state))
        }
    }

    fun `test adding a group preserves a true evaluation`() {
        repeat(iterations) {
            val specs = randomGroupSpecList()
            val extra = randomGroupSpec()
            val state = randomIndexState()
            val base = groupedAction(specs)
            if (base.evaluate(state)) {
                assertTrue(
                    "Adding a group must not turn a true evaluation false",
                    groupedAction(specs + extra).evaluate(state),
                )
            }
        }
    }
}
