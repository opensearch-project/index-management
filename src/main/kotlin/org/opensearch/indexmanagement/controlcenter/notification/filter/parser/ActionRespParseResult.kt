/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult

data class ActionRespParseResult(val operationResult: OperationResult, val message: String, val title: String)
