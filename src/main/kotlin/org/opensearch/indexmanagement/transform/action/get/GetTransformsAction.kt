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

package org.opensearch.indexmanagement.transform.action.get

import org.opensearch.action.ActionType

class GetTransformsAction private constructor() : ActionType<GetTransformsResponse>(NAME, ::GetTransformsResponse) {
    companion object {
        val INSTANCE = GetTransformsAction()
        const val NAME = "cluster:admin/opendistro/transform/get_transforms"
    }
}
