/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.apache.lucene.util.automaton.Operations
import org.opensearch.OpenSearchException
import org.opensearch.common.Strings
import org.opensearch.common.ValidationException
import org.opensearch.common.regex.Regex
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.util.IndexManagementException

/**
 * validate the template Name and indexPattern provided in the template
 *
 * get the idea from ES validate function in MetadataIndexTemplateService
 * acknowledge https://github.com/a2lin who should be the first contributor
 */
@Suppress("ComplexMethod")
fun validateFormat(indexPatterns: List<String>): OpenSearchException? {
    val indexPatternFormatErrors = mutableListOf<String>()
    for (indexPattern in indexPatterns) {
        if (indexPattern.contains("#")) {
            indexPatternFormatErrors.add("index_pattern [$indexPattern] must not contain a '#'")
        }
        if (indexPattern.contains(":")) {
            indexPatternFormatErrors.add("index_pattern [$indexPattern] must not contain a ':'")
        }
        if (indexPattern.startsWith("_")) {
            indexPatternFormatErrors.add("index_pattern [$indexPattern] must not start with '_'")
        }
        if (!Strings.validFileNameExcludingAstrix(indexPattern)) {
            indexPatternFormatErrors.add(
                "index_pattern [" + indexPattern + "] must not contain the following characters " +
                    Strings.INVALID_FILENAME_CHARS
            )
        }
    }

    if (indexPatternFormatErrors.size > 0) {
        val validationException = ValidationException()
        validationException.addValidationErrors(indexPatternFormatErrors)
        return IndexManagementException.wrap(validationException)
    }
    return null
}

fun List<ISMTemplate>.findSelfConflictingTemplates(): Pair<List<String>, List<String>>? {
    val priorityToTemplates = mutableMapOf<Int, List<ISMTemplate>>()
    this.forEach {
        val templateList = priorityToTemplates[it.priority]
        if (templateList != null) {
            priorityToTemplates[it.priority] = templateList.plus(it)
        } else {
            priorityToTemplates[it.priority] = mutableListOf(it)
        }
    }
    priorityToTemplates.forEach { (_, templateList) ->
        // same priority
        val indexPatternsList = templateList.map { it.indexPatterns }
        if (indexPatternsList.size > 1) {
            indexPatternsList.forEachIndexed { ind, indexPatterns ->
                val comparePatterns = indexPatternsList.subList(ind + 1, indexPatternsList.size).flatten()
                if (overlapping(indexPatterns, comparePatterns)) {
                    return indexPatterns to comparePatterns
                }
            }
        }
    }

    return null
}

@Suppress("SpreadOperator")
fun overlapping(p1: List<String>, p2: List<String>): Boolean {
    if (p1.isEmpty() || p2.isEmpty()) return false
    val a1 = Regex.simpleMatchToAutomaton(*p1.toTypedArray())
    val a2 = Regex.simpleMatchToAutomaton(*p2.toTypedArray())
    return !Operations.isEmpty(Operations.intersection(a1, a2))
}

/**
 * find policy templates whose index patterns overlap with given template
 *
 * @return map of overlapping template name to its index patterns
 */
fun Map<String, List<ISMTemplate>>.findConflictingPolicyTemplates(
    candidate: String,
    indexPatterns: List<String>,
    priority: Int
): Map<String, List<String>> {
    val overlappingTemplates = mutableMapOf<String, List<String>>()

    this.forEach { (policyID, templateList) ->
        templateList.filter { it.priority == priority }
            .map { it.indexPatterns }
            .forEach {
                if (overlapping(indexPatterns, it)) {
                    overlappingTemplates[policyID] = it
                }
            }
    }
    overlappingTemplates.remove(candidate)
    return overlappingTemplates
}
