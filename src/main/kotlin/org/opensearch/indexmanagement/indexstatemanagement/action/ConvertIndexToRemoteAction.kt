/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.Version
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.restore.AttemptRestoreStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ConvertIndexToRemoteAction(
    val repository: String,
    val snapshot: String,
    val includeAliases: Boolean = false,
    val ignoreIndexSettings: String = "",
    val numberOfReplicas: Int? = null,
    val deleteOriginalIndex: Boolean = false,
    val addOriginalNameAsAlias: Boolean = false,
    val renamePattern: String = DEFAULT_RENAME_PATTERN,
    index: Int,
) : Action(name, index) {

    init {
        require(!addOriginalNameAsAlias || deleteOriginalIndex) {
            "add_original_name_as_alias can only be used when delete_original_index is true"
        }
    }

    companion object {
        const val name = "convert_index_to_remote"
        const val REPOSITORY_FIELD = "repository"
        const val SNAPSHOT_FIELD = "snapshot"
        const val INCLUDE_ALIASES_FIELD = "include_aliases"
        const val IGNORE_INDEX_SETTINGS_FIELD = "ignore_index_settings"
        const val NUMBER_OF_REPLICAS_FIELD = "number_of_replicas"
        const val DELETE_ORIGINAL_INDEX_FIELD = "delete_original_index"
        const val ADD_ORIGINAL_NAME_AS_ALIAS_FIELD = "add_original_name_as_alias"
        const val RENAME_PATTERN_FIELD = "rename_pattern"
        const val DEFAULT_RENAME_PATTERN = "\$1_remote"

        val VERSION_WITH_RESTORE_OPTIONS = Version.V_3_7_0

        // TODO: Replace Version.CURRENT with the actual version constant (e.g., Version.V_3_8_0) once it is defined in OpenSearch core
        val VERSION_WITH_ADD_ORIGINAL_ALIAS = Version.CURRENT

        val VERSION_WITH_RENAME_PATTERN = Version.V_3_5_0
    }

    private val attemptRestoreStep = AttemptRestoreStep(this)

    private val steps = listOf(attemptRestoreStep)

    override fun getStepToExecute(context: StepContext): Step = attemptRestoreStep

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(REPOSITORY_FIELD, repository)
        builder.field(SNAPSHOT_FIELD, snapshot)
        builder.field(INCLUDE_ALIASES_FIELD, includeAliases)
        builder.field(IGNORE_INDEX_SETTINGS_FIELD, ignoreIndexSettings)
        if (numberOfReplicas != null) {
            builder.field(NUMBER_OF_REPLICAS_FIELD, numberOfReplicas)
        }
        builder.field(DELETE_ORIGINAL_INDEX_FIELD, deleteOriginalIndex)
        builder.field(ADD_ORIGINAL_NAME_AS_ALIAS_FIELD, addOriginalNameAsAlias)
        if (renamePattern != DEFAULT_RENAME_PATTERN) {
            builder.field(RENAME_PATTERN_FIELD, renamePattern)
        }
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeString(repository)
        out.writeString(snapshot)
        if (out.version.onOrAfter(VERSION_WITH_RESTORE_OPTIONS)) {
            out.writeBoolean(includeAliases)
            out.writeString(ignoreIndexSettings)
            out.writeInt(numberOfReplicas ?: -1)
            out.writeBoolean(deleteOriginalIndex)
        }
        if (out.version.onOrAfter(VERSION_WITH_ADD_ORIGINAL_ALIAS)) {
            out.writeBoolean(addOriginalNameAsAlias)
        }
        if (out.version.onOrAfter(VERSION_WITH_RENAME_PATTERN)) {
            out.writeString(renamePattern)
        }
        out.writeInt(actionIndex)
    }
}
