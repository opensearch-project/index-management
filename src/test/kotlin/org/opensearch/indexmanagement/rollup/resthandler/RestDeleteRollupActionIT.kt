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

package org.opensearch.indexmanagement.rollup.resthandler

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.rollup.RollupRestTestCase
import org.opensearch.client.ResponseException
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestDeleteRollupActionIT : RollupRestTestCase() {
    @Throws(Exception::class)
    fun `test deleting a rollup`() {
        val rollup = createRandomRollup()

        val deleteResponse = client().makeRequest("DELETE", "$ROLLUP_JOBS_BASE_URI/${rollup.id}?refresh=true")
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        val getResponse = client().makeRequest("HEAD", "$ROLLUP_JOBS_BASE_URI/${rollup.id}")
        assertEquals("Deleted rollup still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    @Throws(Exception::class)
    fun `test deleting a rollup that doesn't exist in existing config index`() {
        createRandomRollup()
        val res = client().makeRequest("DELETE", "$ROLLUP_JOBS_BASE_URI/foobarbaz")
        assertEquals("Was not not_found response", "not_found", res.asMap()["result"])
    }

    @Throws(Exception::class)
    fun `test deleting a rollup that doesn't exist and config index doesnt exist`() {
        try {
            deleteIndex(INDEX_MANAGEMENT_INDEX)
            val res = client().makeRequest("DELETE", "$ROLLUP_JOBS_BASE_URI/foobarbaz")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
