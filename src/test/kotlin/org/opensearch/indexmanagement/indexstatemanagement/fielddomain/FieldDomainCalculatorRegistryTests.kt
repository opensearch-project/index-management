/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.fielddomain

import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.index.fielddomain.FieldDomain
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class FieldDomainCalculatorRegistryTests : OpenSearchTestCase() {
    fun `test registry returns calculator by type`() {
        val calculator = TestCalculator("test_type")
        val registry = FieldDomainCalculatorRegistry(listOf(calculator))

        assertSame(calculator, registry.get("test_type"))
        assertNull(registry.get("missing_type"))
    }

    fun `test registry rejects duplicate calculator types`() {
        assertFailsWith(IllegalArgumentException::class) {
            FieldDomainCalculatorRegistry(listOf(TestCalculator("test_type"), TestCalculator("test_type")))
        }
    }

    private class TestCalculator(override val type: String) : FieldDomainCalculator {
        override suspend fun calculate(
            context: StepContext,
            indexMetadata: IndexMetadata,
            config: FieldDomainConfig,
            finalized: Boolean,
        ): FieldDomain? = null
    }
}
