package org.opensearch.indexmanagement.adminpanel.notification.action.delete

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.util.NO_ID
import java.io.IOException

class DeleteLRONConfigRequest : ActionRequest {
    val taskID: String
    val refreshPolicy: WriteRequest.RefreshPolicy

    constructor(
        taskID: String = NO_ID,
        refreshPolicy: WriteRequest.RefreshPolicy
    ) : super() {
        this.taskID = taskID
        this.refreshPolicy = refreshPolicy
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        taskID = sin.readString(),
        refreshPolicy = sin.readEnum(WriteRequest.RefreshPolicy::class.java)
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(taskID)
        out.writeEnum(refreshPolicy)
    }
}
