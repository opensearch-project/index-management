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

package org.opensearch.indexmanagement.transform.action.preview

import org.opensearch.action.ActionType

class PreviewTransformAction private constructor() : ActionType<PreviewTransformResponse>(NAME, ::PreviewTransformResponse) {
    companion object {
        val INSTANCE = PreviewTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/preview"
    }
}
