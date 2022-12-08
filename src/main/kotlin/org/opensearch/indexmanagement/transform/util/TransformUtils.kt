import org.opensearch.common.time.DateFormatter
import java.time.ZoneId

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

private const val DATE_FORMAT = "uuuu-MM-dd'T'HH:mm:ss.SSSZZ"
private val timezone: ZoneId = ZoneId.of("UTC")
private val dateFormatter = DateFormatter.forPattern(DATE_FORMAT).withZone(timezone)

fun formatMillis(
    dateTimeInMillis: Long,
): String = dateFormatter.formatMillis(dateTimeInMillis)
