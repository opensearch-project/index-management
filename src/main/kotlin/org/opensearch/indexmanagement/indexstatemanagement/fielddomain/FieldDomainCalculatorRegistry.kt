/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.fielddomain

/**
 * Registry of ISM field-domain calculators keyed by metadata type.
 */
class FieldDomainCalculatorRegistry(
    calculators: List<FieldDomainCalculator>,
) {
    private val calculatorsByType: Map<String, FieldDomainCalculator> =
        calculators.associateBy { it.type }.also {
            require(it.size == calculators.size) { "Field domain calculators must have unique types" }
        }

    fun get(type: String): FieldDomainCalculator? = calculatorsByType[type]

    companion object {
        private val DEFAULT = FieldDomainCalculatorRegistry(
            listOf(DateRangeFieldDomainCalculator()),
        )

        fun defaultRegistry(): FieldDomainCalculatorRegistry = DEFAULT
    }
}
