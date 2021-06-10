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
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.Strings
import org.opensearch.common.ValidationException
import org.opensearch.common.regex.Regex
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.util.IndexManagementException

private val log = LogManager.getLogger("ISMTemplateService")

/**
 * find the matching policy based on ISM template field for the given index
 *
 * filter out hidden index
 * filter out older index than template lastUpdateTime
 *
 * @param ismTemplates current ISM templates saved in metadata
 * @param indexMetadata cluster state index metadata
 * @return policyID
 */
@Suppress("ReturnCount")
fun Map<String, ISMTemplate>.findMatchingPolicy(clusterState: ClusterState, indexName: String): String? {
    if (this.isEmpty()) return null

    val indexMetadata = clusterState.metadata.index(indexName)
    val indexAbstraction = clusterState.metadata.indicesLookup[indexName]
    val isDataStreamIndex = indexAbstraction?.parentDataStream != null

    // Don't include hidden index unless it belongs to a data stream.
    val isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.settings)
    if (!isDataStreamIndex && isHidden) return null

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
    this.filter { (_, template) ->
        template.lastUpdatedTime.toEpochMilli() < indexMetadata.creationDate
    }.forEach { (policyID, template) ->
        val matched = template.indexPatterns.stream().anyMatch(patternMatchPredicate)
        if (matched && highestPriority < template.priority) {
            highestPriority = template.priority
            matchedPolicy = policyID
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

/**
 * find policy templates whose index patterns overlap with given template
 *
 * @return map of overlapping template name to its index patterns
 */
@Suppress("SpreadOperator")
fun Map<String, ISMTemplate>.findConflictingPolicyTemplates(
    candidate: String,
    indexPatterns: List<String>,
    priority: Int
): Map<String, List<String>> {
    val automaton1 = Regex.simpleMatchToAutomaton(*indexPatterns.toTypedArray())
    val overlappingTemplates = mutableMapOf<String, List<String>>()

    // focus on template with same priority
    this.filter { it.value.priority == priority }
        .forEach { (policyID, template) ->
            val automaton2 = Regex.simpleMatchToAutomaton(*template.indexPatterns.toTypedArray())
            if (!Operations.isEmpty(Operations.intersection(automaton1, automaton2))) {
                log.info("Existing ism_template for $policyID overlaps candidate $candidate")
                overlappingTemplates[policyID] = template.indexPatterns
            }
        }
    overlappingTemplates.remove(candidate)

    return overlappingTemplates
}
