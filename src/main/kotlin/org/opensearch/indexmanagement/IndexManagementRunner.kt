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

package org.opensearch.indexmanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.rollup.RollupRunner
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.transform.TransformRunner
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.ScheduledJobRunner

object IndexManagementRunner : ScheduledJobRunner {

    private val logger = LogManager.getLogger(javaClass)

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        when (job) {
            is ManagedIndexConfig -> ManagedIndexRunner.runJob(job, context)
            is Rollup -> RollupRunner.runJob(job, context)
            is Transform -> TransformRunner.runJob(job, context)
            else -> {
                val errorMessage = "Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}"
                logger.error(errorMessage)
                throw IllegalArgumentException(errorMessage)
            }
        }
    }
}
