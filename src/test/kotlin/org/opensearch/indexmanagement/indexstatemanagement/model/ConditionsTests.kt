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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.indexmanagement.indexstatemanagement.randomByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.randomTimeValueObject
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class ConditionsTests : OpenSearchTestCase() {

    fun `test supplying more than one transition condition fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for supplying multiple transition conditions") {
            Conditions(indexAge = randomTimeValueObject(), size = randomByteSizeValue())
        }
    }

    fun `test doc count condition of zero fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for doc count condition less than 1") {
            Conditions(docCount = 0)
        }
    }

    fun `test size condition of zero fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for size condition less than 1") {
            Conditions(size = ByteSizeValue.parseBytesSizeValue("0", "size_test"))
        }
    }
}
