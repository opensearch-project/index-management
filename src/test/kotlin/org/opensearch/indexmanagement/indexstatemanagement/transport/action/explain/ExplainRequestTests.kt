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
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.model.SearchParams
import org.opensearch.test.OpenSearchTestCase

class ExplainRequestTests : OpenSearchTestCase() {

    fun `test explain request`() {
        val indices = listOf("index1", "index2")
        val local = true
        val masterTimeout = TimeValue.timeValueSeconds(30)
        val params = SearchParams(0, 20, "sort-field", "asc", "*")
        val req = ExplainRequest(indices, local, masterTimeout, params)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = ExplainRequest(sin)
        assertEquals(indices, newReq.indices)
        assertEquals(local, newReq.local)
    }
}
