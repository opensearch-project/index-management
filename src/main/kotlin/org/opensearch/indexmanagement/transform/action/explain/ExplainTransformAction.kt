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

package org.opensearch.indexmanagement.transform.action.explain

import org.opensearch.action.ActionType

class ExplainTransformAction private constructor() : ActionType<ExplainTransformResponse>(NAME, ::ExplainTransformResponse) {
    companion object {
        val INSTANCE = ExplainTransformAction()
        val NAME = "cluster:admin/opendistro/transform/explain"
    }
}
