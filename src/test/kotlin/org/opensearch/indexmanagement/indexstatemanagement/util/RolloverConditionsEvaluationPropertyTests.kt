/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.util

import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.filter
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.long
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverConditionGroup
import org.opensearch.test.OpenSearchTestCase

/**
 * Property-based tests for [RolloverAction.evaluateConditions] covering the OR-of-ANDs grouped semantics
 * and the preserved flat (implicit OR) semantics.
 *
 * Each test runs at least 100 generated iterations via kotest-property's checkAll.
 */
class RolloverConditionsEvaluationPropertyTests : OpenSearchTestCase() {

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

    private val indexStateArb: Arb<IndexState> = arbitrary {
        IndexState(
            indexAge = TimeValue.timeValueMillis(Arb.long(0L..100L).bind()),
            numDocs = Arb.long(0L..100L).bind(),
            indexSize = ByteSizeValue(Arb.long(0L..100L).bind()),
            primaryShardSize = ByteSizeValue(Arb.long(0L..100L).bind()),
        )
    }

    private val flatSpecArb: Arb<ConditionSpec> = arbitrary {
        ConditionSpec(
            minSize = if (Arb.boolean().bind()) ByteSizeValue(Arb.long(1L..100L).bind()) else null,
            minDocs = if (Arb.boolean().bind()) Arb.long(1L..100L).bind() else null,
            minAge = if (Arb.boolean().bind()) TimeValue.timeValueMillis(Arb.long(1L..100L).bind()) else null,
            minPrimaryShardSize = if (Arb.boolean().bind()) ByteSizeValue(Arb.long(1L..100L).bind()) else null,
        )
    }

    private val groupSpecArb: Arb<ConditionSpec> = flatSpecArb.filter { !it.isEmpty() }

    private val singleConditionSpecArb: Arb<ConditionSpec> = arbitrary {
        when (Arb.int(0..3).bind()) {
            0 -> ConditionSpec(ByteSizeValue(Arb.long(1L..100L).bind()), null, null, null)
            1 -> ConditionSpec(null, Arb.long(1L..100L).bind(), null, null)
            2 -> ConditionSpec(null, null, TimeValue.timeValueMillis(Arb.long(1L..100L).bind()), null)
            else -> ConditionSpec(null, null, null, ByteSizeValue(Arb.long(1L..100L).bind()))
        }
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

    fun `test grouped evaluation matches OR-of-ANDs reference`() = runBlocking {
        checkAll(100, Arb.list(groupSpecArb, 1..4), indexStateArb) { specs, state ->
            val expected = specs.any { spec -> results(spec, state).all { it } }
            assertEquals(
                "Grouped evaluation must equal OR-of-ANDs reference",
                expected,
                groupedAction(specs).evaluate(state),
            )
        }
    }

    fun `test flat evaluation matches any-condition-satisfied reference`() = runBlocking {
        checkAll(100, flatSpecArb, indexStateArb) { spec, state ->
            val r = results(spec, state)
            val expected = if (r.isEmpty()) true else r.any { it }
            assertEquals(
                "Flat evaluation must equal the OR reference",
                expected,
                flatAction(spec).evaluate(state),
            )
        }
    }

    fun `test single-condition group equals single flat condition`() = runBlocking {
        checkAll(100, singleConditionSpecArb, indexStateArb) { spec, state ->
            assertEquals(
                "A one-condition group must match the equivalent flat condition",
                flatAction(spec).evaluate(state),
                groupedAction(listOf(spec)).evaluate(state),
            )
        }
    }

    fun `test single group equals AND of its conditions`() = runBlocking {
        checkAll(100, groupSpecArb, indexStateArb) { spec, state ->
            val expected = results(spec, state).all { it }
            assertEquals(
                "A single group must be satisfied only when all its conditions hold",
                expected,
                groupedAction(listOf(spec)).evaluate(state),
            )
        }
    }

    fun `test no conditions at all evaluates true`() = runBlocking {
        checkAll(100, indexStateArb) { state ->
            val action = RolloverAction(
                minSize = null, minDocs = null, minAge = null, minPrimaryShardSize = null,
                preventEmptyRollover = false, index = 0,
            )
            assertTrue("An action with no conditions must always evaluate true", action.evaluate(state))
        }
    }

    fun `test adding a group preserves a true evaluation`() = runBlocking {
        checkAll(100, Arb.list(groupSpecArb, 1..4), groupSpecArb, indexStateArb) { specs, extra, state ->
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
