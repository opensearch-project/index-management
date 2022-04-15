/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.model

import org.apache.logging.log4j.LogManager
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import java.io.IOException
import java.time.Instant
import java.util.Locale
import kotlin.math.pow

data class ActionRetry(
    val count: Long,
    val backoff: Backoff = Backoff.EXPONENTIAL,
    val delay: TimeValue = TimeValue.timeValueMinutes(1)
) : ToXContentFragment, Writeable {

    init { require(count >= 0) { "Count for ActionRetry must be a non-negative number" } }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject(RETRY_FIELD)
            .field(COUNT_FIELD, count)
            .field(BACKOFF_FIELD, backoff)
            .field(DELAY_FIELD, delay.stringRep)
            .endObject()
        return builder
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        count = sin.readLong(),
        backoff = sin.readEnum(Backoff::class.java),
        delay = sin.readTimeValue()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeLong(count)
        out.writeEnum(backoff)
        out.writeTimeValue(delay)
    }

    companion object {
        const val RETRY_FIELD = "retry"
        const val COUNT_FIELD = "count"
        const val BACKOFF_FIELD = "backoff"
        const val DELAY_FIELD = "delay"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionRetry {
            var count: Long? = null
            var backoff: Backoff = Backoff.EXPONENTIAL
            var delay: TimeValue = TimeValue.timeValueMinutes(1)

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    COUNT_FIELD -> count = xcp.longValue()
                    BACKOFF_FIELD -> backoff = Backoff.valueOf(xcp.text().uppercase(Locale.ROOT))
                    DELAY_FIELD -> delay = TimeValue.parseTimeValue(xcp.text(), DELAY_FIELD)
                }
            }

            return ActionRetry(
                count = requireNotNull(count) { "ActionRetry count is null" },
                backoff = backoff,
                delay = delay
            )
        }
    }

    enum class Backoff(val type: String, val getNextRetryTime: (consumedRetries: Int, timeValue: TimeValue) -> Long) {
        EXPONENTIAL(
            "exponential",
            { consumedRetries, timeValue ->
                (2.0.pow(consumedRetries - 1)).toLong() * timeValue.millis
            }
        ),
        CONSTANT(
            "constant",
            { _, timeValue ->
                timeValue.millis
            }
        ),
        LINEAR(
            "linear",
            { consumedRetries, timeValue ->
                consumedRetries * timeValue.millis
            }
        );

        private val logger = LogManager.getLogger(javaClass)

        override fun toString(): String {
            return type
        }

        @Suppress("ReturnCount")
        fun shouldBackoff(actionMetaData: ActionMetaData?, actionRetry: ActionRetry?): Pair<Boolean, Long?> {
            if (actionMetaData == null || actionRetry == null) {
                logger.debug("There is no actionMetaData and ActionRetry we don't need to backoff")
                return Pair(false, null)
            }

            if (actionMetaData.consumedRetries > 0) {
                if (actionMetaData.lastRetryTime != null) {
                    val remainingTime = getNextRetryTime(actionMetaData.consumedRetries, actionRetry.delay) -
                        (Instant.now().toEpochMilli() - actionMetaData.lastRetryTime)

                    return Pair(remainingTime > 0, remainingTime)
                }
            }

            return Pair(false, null)
        }
    }
}
