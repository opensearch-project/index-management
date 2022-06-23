/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.actionfilter

import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable

/**
 * To support rollup indices being returned in correct format in FieldCaps API we have to rewrite the FieldCapabilitiesResponse
 * with correct rollup index's mappings. However many methods/constructors of FieldCapabilitiesResponse class are package private.
 * To achieve the rewrite despite limitations, the following data classes have been defined so we can modify the ByteStream of
 * FieldCapabilitiesResponse to include correct rollup mappings and convert it back to FieldCapabilitiesResponse.
 *
 * TODO: When/if FieldCapabilitiesResponse and other subclasses package private constructors are elevated to public we can remove this logic.
 */

class ISMFieldCapabilitiesIndexResponse(
    private val indexName: String,
    private val responseMap: Map<String, ISMIndexFieldCapabilities>,
    private val canMatch: Boolean
) : Writeable {

    constructor(sin: StreamInput) : this(
        indexName = sin.readString(),
        responseMap = sin.readMap({ it.readString() }, { ISMIndexFieldCapabilities(it) }),
        canMatch = sin.readBoolean()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(indexName)
        out.writeMap(
            responseMap,
            { writer, value -> writer.writeString(value) },
            { writer, value -> value.writeTo(writer) }
        )
        out.writeBoolean(canMatch)
    }
}

class ISMFieldCapabilitiesResponse(
    val indices: Array<String>,
    val responseMap: Map<String, Map<String, ISMFieldCapabilities>>,
    val indexResponses: List<ISMFieldCapabilitiesIndexResponse>
) {

    fun toFieldCapabilitiesResponse(): FieldCapabilitiesResponse {
        val out = BytesStreamOutput()
        out.writeStringArray(indices)
        out.writeMap(
            responseMap,
            { writer, value -> writer.writeString(value) },
            { writer, value -> writer.writeMap(value, { w, v -> w.writeString(v) }, { w, v -> v.writeTo(w) }) }
        )
        out.writeList(indexResponses)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        return FieldCapabilitiesResponse(sin)
    }

    companion object {
        fun fromFieldCapabilitiesResponse(response: FieldCapabilitiesResponse): ISMFieldCapabilitiesResponse {
            val out = BytesStreamOutput().also { response.writeTo(it) }
            val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
            val indices = sin.readStringArray()
            val responseMap = sin.readMap({ it.readString() }, { it.readMap({ it.readString() }, ::ISMFieldCapabilities) })
            val indexResponses = sin.readList { ISMFieldCapabilitiesIndexResponse(it) }
            return ISMFieldCapabilitiesResponse(indices, responseMap, indexResponses)
        }
    }
}

@Suppress("LongParameterList")
class ISMFieldCapabilities(
    private val name: String,
    private val type: String,
    private val isSearchable: Boolean,
    private val isAggregatable: Boolean,
    private val indices: Array<String>?,
    private val nonSearchableIndices: Array<String>?,
    private val nonAggregatableIndices: Array<String>?,
    private val meta: Map<String, Set<String>>
) : Writeable {

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeString(type)
        out.writeBoolean(isSearchable)
        out.writeBoolean(isAggregatable)
        out.writeOptionalStringArray(indices)
        out.writeOptionalStringArray(nonSearchableIndices)
        out.writeOptionalStringArray(nonAggregatableIndices)
        out.writeMap(
            meta,
            { writer, value -> writer.writeString(value) },
            { writer, value -> writer.writeCollection(value) { w, v -> w.writeString(v) } }
        )
    }

    constructor(sin: StreamInput) : this(
        name = sin.readString(),
        type = sin.readString(),
        isSearchable = sin.readBoolean(),
        isAggregatable = sin.readBoolean(),
        indices = sin.readOptionalStringArray(),
        nonSearchableIndices = sin.readOptionalStringArray(),
        nonAggregatableIndices = sin.readOptionalStringArray(),
        meta = sin.readMap({ it.readString() }, { it.readSet { it.readString() } })
    )
}

class ISMIndexFieldCapabilities(
    private val name: String,
    private val type: String,
    private val isSearchable: Boolean,
    private val isAggregatable: Boolean,
    private val meta: Map<String, String>
) : Writeable {

    constructor(sin: StreamInput) : this(
        name = sin.readString(),
        type = sin.readString(),
        isSearchable = sin.readBoolean(),
        isAggregatable = sin.readBoolean(),
        meta = sin.readMap({ it.readString() }, { it.readString() })
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeString(type)
        out.writeBoolean(isSearchable)
        out.writeBoolean(isAggregatable)
        out.writeMap(
            meta,
            { writer, value: String -> writer.writeString(value) },
            { writer, value: String -> writer.writeString(value) }
        )
    }
}
