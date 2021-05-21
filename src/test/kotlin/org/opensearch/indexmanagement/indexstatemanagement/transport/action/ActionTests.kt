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

package org.opensearch.indexmanagement.indexstatemanagement.transport.action

import org.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexAction
import org.opensearch.test.OpenSearchTestCase

class ActionTests : OpenSearchTestCase() {
    fun `test add policy action name`() {
        assertNotNull(AddPolicyAction.INSTANCE.name())
        assertEquals(AddPolicyAction.INSTANCE.name(), AddPolicyAction.NAME)
    }

    fun `test remove policy action name`() {
        assertNotNull(RemovePolicyAction.INSTANCE.name())
        assertEquals(RemovePolicyAction.INSTANCE.name(), RemovePolicyAction.NAME)
    }

    fun `test retry failed managed index action name`() {
        assertNotNull(RetryFailedManagedIndexAction.INSTANCE.name())
        assertEquals(RetryFailedManagedIndexAction.INSTANCE.name(), RetryFailedManagedIndexAction.NAME)
    }

    fun `test change policy action name`() {
        assertNotNull(ChangePolicyAction.NAME)
        assertEquals(ChangePolicyAction.INSTANCE.name(), ChangePolicyAction.NAME)
    }

    fun `test index policy action name`() {
        assertNotNull(IndexPolicyAction.NAME)
        assertEquals(IndexPolicyAction.INSTANCE.name(), IndexPolicyAction.NAME)
    }

    fun `test explain action name`() {
        assertNotNull(ExplainAction.NAME)
        assertEquals(ExplainAction.INSTANCE.name(), ExplainAction.NAME)
    }

    fun `test delete policy action name`() {
        assertNotNull(DeletePolicyAction.NAME)
        assertEquals(DeletePolicyAction.INSTANCE.name(), DeletePolicyAction.NAME)
    }

    fun `test get policy action name`() {
        assertNotNull(GetPolicyAction.NAME)
        assertEquals(GetPolicyAction.INSTANCE.name(), GetPolicyAction.NAME)
    }

    fun `test get policies action name`() {
        assertNotNull(GetPoliciesAction.NAME)
        assertEquals(GetPoliciesAction.INSTANCE.name(), GetPoliciesAction.NAME)
    }
}
