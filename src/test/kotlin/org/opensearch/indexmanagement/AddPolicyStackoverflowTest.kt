/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

class AddPolicyStackoverflowTest : ISMSingleNodeTestCase() {

    fun `test add policy with many indices`() {
        var indexNamePrefix = "index-"
        for (i in 1..5) {
            createIndex("$indexNamePrefix$i")
        }
    }
}
