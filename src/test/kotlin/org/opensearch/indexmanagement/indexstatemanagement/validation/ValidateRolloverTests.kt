/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import com.nhaarman.mockitokotlin2.mock
import kotlinx.coroutines.runBlocking
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase

class ValidateRolloverTests : OpenSearchTestCase() {
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val clusterService: ClusterService = mock()
    private val indexName: String = "test"
    private val metadata = ManagedIndexMetaData(
        indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
        ActionMetaData
        ("rollover", 1, 0, false, 0, null, null),
        null, null, null
    )
    val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)

    private val client: Client = mock()
    private val lockService: LockService = LockService(mock(), clusterService)
    private val validate = ValidateRollover(settings, clusterService)

    fun `test rollover when missing rollover alias`() {
        val actionMetadata = metadata.actionMetaData!!.copy()
        val metadata = metadata.copy()
        val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)

        // null pointer exception
        runBlocking {
            validate.executeValidation(context)
        }

        validate.getUpdatedManagedIndexMetadata(metadata, actionMetadata)
        assertEquals("Validation status is REVALIDATE", Validate.ValidationStatus.REVALIDATE, validate.validationStatus)
    }
}
