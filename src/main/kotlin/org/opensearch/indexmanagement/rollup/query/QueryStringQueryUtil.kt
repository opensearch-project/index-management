/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.query

import org.apache.lucene.queryparser.classic.ParseException
import org.apache.lucene.queryparser.classic.QueryParser
import org.opensearch.OpenSearchParseException
import org.opensearch.common.regex.Regex
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.index.analysis.NamedAnalyzer
import org.opensearch.index.query.QueryBuilder
import org.opensearch.index.query.QueryShardContext
import org.opensearch.index.query.QueryShardException
import org.opensearch.index.query.QueryStringQueryBuilder
import org.opensearch.index.query.support.QueryParsers
import org.opensearch.index.search.QueryParserHelper
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.rollup.util.QueryShardContextFactory

object QueryStringQueryUtil {

    fun rewriteQueryStringQuery(
        queryBuilder: QueryBuilder,
        sourceIndexMappings: Map<String, Any>
    ): QueryStringQueryBuilder {
        val qsqBuilder = queryBuilder as QueryStringQueryBuilder
        // Parse query_string query and extract all discovered fields
        val (fieldsFromQueryString, otherFields) = extractFieldsFromQueryString(queryBuilder, sourceIndexMappings)
        // Rewrite query_string
        var newQueryString = qsqBuilder.queryString()
        fieldsFromQueryString.forEach { field ->
            val escapedField = escapeSpaceCharacters(field)
            newQueryString = newQueryString.replace("$escapedField:", "$escapedField.${Dimension.Type.TERMS.type}:")
            newQueryString = newQueryString.replace("$EXISTS:$escapedField", "$EXISTS:$escapedField.${Dimension.Type.TERMS.type}")
        }

        // We will rewrite here only concrete default fields.
        // Prefix ones we will resolve(otherFields) and insert into fields array
        var newDefaultField = qsqBuilder.defaultField()
        if (newDefaultField != null && Regex.isSimpleMatchPattern(newDefaultField) == false) {
            newDefaultField = newDefaultField + ".${Dimension.Type.TERMS.type}"
        } else {
            newDefaultField = null
        }

        var newFields: MutableMap<String, Float>? = null
        if (otherFields.isNotEmpty()) {
            newFields = mutableMapOf()
            otherFields.forEach {
                newFields.put("${it.key}.${Dimension.Type.TERMS.type}", it.value)
            }
        }
        var retVal = QueryStringQueryBuilder(newQueryString)
            .rewrite(qsqBuilder.rewrite())
            .fuzzyRewrite(qsqBuilder.fuzzyRewrite())
            .autoGenerateSynonymsPhraseQuery(qsqBuilder.autoGenerateSynonymsPhraseQuery())
            .allowLeadingWildcard(qsqBuilder.allowLeadingWildcard())
            .analyzeWildcard(qsqBuilder.analyzeWildcard())
            .defaultOperator(qsqBuilder.defaultOperator())
            .escape(qsqBuilder.escape())
            .fuzziness(qsqBuilder.fuzziness())
            .lenient(qsqBuilder.lenient())
            .enablePositionIncrements(qsqBuilder.enablePositionIncrements())
            .fuzzyMaxExpansions(qsqBuilder.fuzzyMaxExpansions())
            .fuzzyPrefixLength(qsqBuilder.fuzzyPrefixLength())
            .queryName(qsqBuilder.queryName())
            .quoteAnalyzer(qsqBuilder.quoteAnalyzer())
            .analyzer(qsqBuilder.analyzer())
            .minimumShouldMatch(qsqBuilder.minimumShouldMatch())
            .timeZone(qsqBuilder.timeZone())
            .phraseSlop(qsqBuilder.phraseSlop())
            .quoteFieldSuffix(qsqBuilder.quoteFieldSuffix())
            .boost(qsqBuilder.boost())
            .fuzzyTranspositions(qsqBuilder.fuzzyTranspositions())

        if (newDefaultField != null) {
            retVal = retVal.defaultField(newDefaultField)
        } else if (newFields != null && newFields.size > 0) {
            retVal = retVal.fields(newFields)
        }
        if (qsqBuilder.tieBreaker() != null) {
            retVal = retVal.tieBreaker(qsqBuilder.tieBreaker())
        }
        return retVal
    }

    private fun escapeSpaceCharacters(field: String): String {
        val escapedField = field.replace(" ", "\\ ")
        return escapedField
    }

    @Suppress("ComplexMethod", "LongMethod", "ThrowsCount", "EmptyCatchBlock")
    fun extractFieldsFromQueryString(queryBuilder: QueryBuilder, sourceIndexMappings: Map<String, Any>): Pair<List<String>, Map<String, Float>> {
        val context = QueryShardContextFactory.createShardContext(sourceIndexMappings)
        val qsqBuilder = queryBuilder as QueryStringQueryBuilder
        val rewrittenQueryString = if (qsqBuilder.escape()) QueryParser.escape(qsqBuilder.queryString()) else qsqBuilder.queryString()
        val queryParser: QueryStringQueryParserExt
        val isLenient: Boolean = if (qsqBuilder.lenient() == null) context.queryStringLenient() else qsqBuilder.lenient()
        var otherFields = mapOf<String, Float>()
        if (qsqBuilder.defaultField() != null) {
            if (Regex.isMatchAllPattern(qsqBuilder.defaultField())) {
                otherFields = resolveMatchPatternFields(context)
                queryParser = QueryStringQueryParserExt(context, if (qsqBuilder.lenient() == null) true else qsqBuilder.lenient())
            } else if (Regex.isSimpleMatchPattern(qsqBuilder.defaultField())) {
                otherFields = resolveMatchPatternFields(context, qsqBuilder.defaultField())
                queryParser = QueryStringQueryParserExt(context, qsqBuilder.defaultField(), isLenient)
            } else {
                queryParser = QueryStringQueryParserExt(context, qsqBuilder.defaultField(), isLenient)
            }
        } else if (qsqBuilder.fields().size > 0) {
            val resolvedFields = QueryParserHelper.resolveMappingFields(context, qsqBuilder.fields())
            otherFields = resolvedFields
            queryParser = if (QueryParserHelper.hasAllFieldsWildcard(qsqBuilder.fields().keys)) {
                QueryStringQueryParserExt(context, resolvedFields, if (qsqBuilder.lenient() == null) true else qsqBuilder.lenient())
            } else {
                QueryStringQueryParserExt(context, resolvedFields, isLenient)
            }
        } else {
            val defaultFields: List<String> = context.defaultFields()
            queryParser = if (QueryParserHelper.hasAllFieldsWildcard(defaultFields)) {
                otherFields = resolveMatchPatternFields(context)
                QueryStringQueryParserExt(context, if (qsqBuilder.lenient() == null) true else qsqBuilder.lenient())
            } else {
                val resolvedFields = QueryParserHelper.resolveMappingFields(
                    context,
                    QueryParserHelper.parseFieldsAndWeights(defaultFields)
                )
                otherFields = resolvedFields
                QueryStringQueryParserExt(context, resolvedFields, isLenient)
            }
        }

        if (qsqBuilder.analyzer() != null) {
            val namedAnalyzer: NamedAnalyzer = context.getIndexAnalyzers().get(qsqBuilder.analyzer())
                ?: throw QueryShardException(context, "[query_string] analyzer [$qsqBuilder.analyzer] not found")
            queryParser.setForceAnalyzer(namedAnalyzer)
        }

        if (qsqBuilder.quoteAnalyzer() != null) {
            val forceQuoteAnalyzer: NamedAnalyzer = context.getIndexAnalyzers().get(qsqBuilder.quoteAnalyzer())
                ?: throw QueryShardException(context, "[query_string] quote_analyzer [$qsqBuilder.quoteAnalyzer] not found")
            queryParser.setForceQuoteAnalyzer(forceQuoteAnalyzer)
        }

        queryParser.defaultOperator = qsqBuilder.defaultOperator().toQueryParserOperator()
        // TODO can we extract this somehow? There's no getter for this
        queryParser.setType(QueryStringQueryBuilder.DEFAULT_TYPE)
        if (qsqBuilder.tieBreaker() != null) {
            queryParser.setGroupTieBreaker(qsqBuilder.tieBreaker())
        } else {
            queryParser.setGroupTieBreaker(QueryStringQueryBuilder.DEFAULT_TYPE.tieBreaker())
        }
        queryParser.phraseSlop = qsqBuilder.phraseSlop()
        queryParser.setQuoteFieldSuffix(qsqBuilder.quoteFieldSuffix())
        queryParser.allowLeadingWildcard =
            if (qsqBuilder.allowLeadingWildcard() == null) context.queryStringAllowLeadingWildcard()
            else qsqBuilder.allowLeadingWildcard()
        queryParser.setAnalyzeWildcard(
            if (qsqBuilder.analyzeWildcard() == null) context.queryStringAnalyzeWildcard()
            else qsqBuilder.analyzeWildcard()
        )
        queryParser.enablePositionIncrements = qsqBuilder.enablePositionIncrements()
        queryParser.setFuzziness(qsqBuilder.fuzziness())
        queryParser.fuzzyPrefixLength = qsqBuilder.fuzzyPrefixLength()
        queryParser.setFuzzyMaxExpansions(qsqBuilder.fuzzyMaxExpansions())
        queryParser.setFuzzyRewriteMethod(QueryParsers.parseRewriteMethod(qsqBuilder.fuzzyRewrite(), LoggingDeprecationHandler.INSTANCE))
        queryParser.multiTermRewriteMethod = QueryParsers.parseRewriteMethod(qsqBuilder.rewrite(), LoggingDeprecationHandler.INSTANCE)
        queryParser.setTimeZone(qsqBuilder.timeZone())
        queryParser.determinizeWorkLimit = qsqBuilder.maxDeterminizedStates()
        queryParser.autoGenerateMultiTermSynonymsPhraseQuery = qsqBuilder.autoGenerateSynonymsPhraseQuery()
        queryParser.setFuzzyTranspositions(qsqBuilder.fuzzyTranspositions())

        try {
            queryParser.parse(rewrittenQueryString)
        } catch (e: ParseException) {
            throw IllegalArgumentException("Failed to parse query [" + qsqBuilder.queryString() + "]", e)
        }
        // Return discovered fields
        return queryParser.discoveredFields to if (queryParser.hasLonelyTerms) otherFields else mapOf()
    }

    @Suppress("EmptyCatchBlock", "LoopWithTooManyJumpStatements")
    fun resolveMatchPatternFields(
        context: QueryShardContext,
        pattern: String = "*"
    ): Map<String, Float> {
        val allFields = context.simpleMatchToIndexNames(pattern)
        val fields: MutableMap<String, Float> = HashMap()
        for (fieldName in allFields) {
            val fieldType = context.mapperService.fieldType(fieldName) ?: continue
            if (fieldType.name().startsWith("_")) {
                // Ignore metadata fields
                continue
            }

            try {
                fieldType.termQuery("", context)
            } catch (e: QueryShardException) {
                // field type is never searchable with term queries (eg. geo point): ignore
                continue
            } catch (e: UnsupportedOperationException) {
                continue
            } catch (e: IllegalArgumentException) {
                // other exceptions are parsing errors or not indexed fields: keep
            } catch (e: OpenSearchParseException) {
            }

            // Deduplicate aliases and their concrete fields.
            val resolvedFieldName = fieldType.name()
            if (allFields.contains(resolvedFieldName)) {
                fields[fieldName] = 1.0f
            }
        }
        return fields
    }
}
