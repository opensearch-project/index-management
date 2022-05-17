/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.apache.logging.log4j.LogManager

private val log = LogManager.getLogger("Snapshot Management Helper")

const val smSuffix = "-sm"
fun smPolicyNameToDocId(policyName: String) = "$policyName$smSuffix"
fun smDocIdToPolicyName(id: String) = id.substringBeforeLast(smSuffix)
fun getSMMetadataDocId(policyName: String) = "$policyName-sm-metadata"
