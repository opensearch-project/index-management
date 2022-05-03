/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.apache.logging.log4j.LogManager

private val log = LogManager.getLogger("Snapshot Management Helper")

fun getSMDocId(policyName: String) = "$policyName-sm"
fun getSMMetadataDocId(policyName: String) = "$policyName-sm-metadata"
