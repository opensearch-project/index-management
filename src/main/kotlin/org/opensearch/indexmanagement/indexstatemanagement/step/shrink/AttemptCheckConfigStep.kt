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

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ShrinkActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step

class AttemptCheckConfigStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ShrinkActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    @Suppress("TooGenericExceptionCaught", "ComplexMethod")
    override suspend fun execute(): AttemptCheckConfigStep {
        return this
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        // Saving maxNumSegments in ActionProperties after the force merge operation has begun so that if a ChangePolicy occurred
        // in between this step and WaitForForceMergeStep, a cached segment count expected from the operation is available
        val currentActionMetaData = currentMetaData.actionMetaData
        return currentMetaData.copy(
            actionMetaData = currentActionMetaData?.copy(),
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "attempt_check_config_step"
        const val FIVE_MINUTES_IN_MILLIS = 1000 * 60 * 5 // how long to wait for the force merge request before moving on
        const val FIVE_SECONDS_IN_MILLIS = 1000L * 5L // delay
    }
}
