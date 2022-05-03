/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.rollup.RollupRunner
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.snapshotmanagement.SMRunner
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
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
            is SMPolicy -> SMRunner.runJob(job, context)
            else -> {
                val errorMessage = "Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}"
                logger.error(errorMessage)
                throw IllegalArgumentException(errorMessage)
            }
        }
    }
}
