/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.get

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.util.NO_ID
import java.io.IOException

/* There are default lronConfigs and lronConfigs binded with taskID */
/* If includeDefault == true, this request will get the config with highest priority */
/* else, it will only get the config binded with taskID */

class GetLRONConfigRequest : ActionRequest {
    val taskID: String
    val includeDefault: Boolean

    constructor(
        taskID: String = NO_ID,
        includeDefault: Boolean = false
    ) : super() {
        this.taskID = taskID
        this.includeDefault = includeDefault
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        taskID = sin.readString(),
        includeDefault = sin.readBoolean()
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (NO_ID == taskID && !includeDefault) {
            validationException = ValidateActions.addValidationError("task id is missing", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(taskID)
        out.writeBoolean(includeDefault)
    }
}
