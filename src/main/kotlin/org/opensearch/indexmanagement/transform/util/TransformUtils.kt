import org.opensearch.common.time.DateFormatter
import java.time.ZoneId

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

private const val dateFormat = "uuuu-MM-dd'T'HH:mm:ss.SSSZZ"
private val timezone: ZoneId = ZoneId.of("UTC")
private val dateFormatter = DateFormatter.forPattern(dateFormat).withZone(timezone)

fun formatMillis(
    targetValueMap: MutableMap<String, Any>,
    mappedDateField: String,
) = dateFormatter.formatMillis(targetValueMap[mappedDateField] as Long)
