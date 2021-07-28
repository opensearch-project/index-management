/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.apache.logging.log4j.LogManager
import org.apache.lucene.util.automaton.Operations
import org.opensearch.OpenSearchException
import org.opensearch.cluster.metadata.IndexAbstraction
import org.opensearch.common.Strings
import org.opensearch.common.ValidationException
import org.opensearch.common.regex.Regex
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.util.IndexManagementException

private val log = LogManager.getLogger("ISMTemplateService")

/**
 * find the matching policy for the given index
 *
 * return early if it's hidden index
 * filter out templates that were last updated after the index creation time
 *
 * @return policyID
 */
@Suppress("ReturnCount", "NestedBlockDepth")
fun Map<String, List<ISMTemplate>>.findMatchingPolicy(
    indexName: String,
    indexCreationDate: Long,
    isHiddenIndex: Boolean,
    indexAbstraction: IndexAbstraction?
): String? {
    val isDataStreamIndex = indexAbstraction?.parentDataStream != null
    if (this.isEmpty()) return null
    // don't include hidden index
    if (!isDataStreamIndex && isHiddenIndex) return null

    // If the index belongs to a data stream, then find the matching policy using the data stream name.
    val lookupName = when {
        isDataStreamIndex -> indexAbstraction?.parentDataStream?.name
        else -> indexName
    }

    // only process indices created after template
    // traverse all ism templates for matching ones
    val patternMatchPredicate = { pattern: String -> Regex.simpleMatch(pattern, lookupName) }
    var matchedPolicy: String? = null
    var highestPriority: Int = -1

    this.forEach { (policyID, templateList) ->
        templateList.filter { it.lastUpdatedTime.toEpochMilli() < indexCreationDate }
            .forEach {
                if (it.indexPatterns.stream().anyMatch(patternMatchPredicate)) {
                    if (highestPriority < it.priority) {
                        highestPriority = it.priority
                        matchedPolicy = policyID
                    } else if (highestPriority == it.priority) {
                        log.warn("Warning: index $lookupName matches [$matchedPolicy, $policyID]")
                    }
                }
            }
    }

    return matchedPolicy
}

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
