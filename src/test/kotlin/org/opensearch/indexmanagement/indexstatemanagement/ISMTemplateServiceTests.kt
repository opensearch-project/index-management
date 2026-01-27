/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.indexmanagement.util.IndexManagementException
import org.opensearch.test.OpenSearchTestCase

class ISMTemplateServiceTests : OpenSearchTestCase() {
    fun `test validateFormat with pattern containing hash`() {
        val patterns = listOf("log#*")
        val exception = validateFormat(patterns)
        assertNotNull("Expected validation error for pattern with #", exception)
        assertTrue(exception is IndexManagementException)
        assertTrue(exception!!.message!!.contains("must not contain a '#'"))
    }

    fun `test validateFormat with exclusion pattern containing hash`() {
        val patterns = listOf("log-*", "-test#*")
        val exception = validateFormat(patterns)
        assertNotNull("Expected validation error for exclusion pattern with #", exception)
        assertTrue(exception is IndexManagementException)
        assertTrue(exception!!.message!!.contains("must not contain a '#'"))
    }

    fun `test validateFormat with pattern containing colon`() {
        val patterns = listOf("log:*")
        val exception = validateFormat(patterns)
        assertNotNull("Expected validation error for pattern with :", exception)
        assertTrue(exception is IndexManagementException)
        assertTrue(exception!!.message!!.contains("must not contain a ':'"))
    }

    fun `test validateFormat with exclusion pattern containing colon`() {
        val patterns = listOf("log-*", "-test:*")
        val exception = validateFormat(patterns)
        assertNotNull("Expected validation error for exclusion pattern with :", exception)
        assertTrue(exception is IndexManagementException)
        assertTrue(exception!!.message!!.contains("must not contain a ':'"))
    }

    fun `test validateFormat with pattern starting with underscore`() {
        val patterns = listOf("_log*")
        val exception = validateFormat(patterns)
        assertNotNull("Expected validation error for pattern starting with _", exception)
        assertTrue(exception is IndexManagementException)
        assertTrue(exception!!.message!!.contains("must not start with '_'"))
    }

    fun `test validateFormat with exclusion pattern starting with underscore`() {
        val patterns = listOf("log-*", "-_test*")
        val exception = validateFormat(patterns)
        assertNotNull("Expected validation error for exclusion pattern starting with _", exception)
        assertTrue(exception is IndexManagementException)
        assertTrue(exception!!.message!!.contains("must not start with '_'"))
    }

    fun `test validateFormat with empty exclusion pattern`() {
        val patterns = listOf("log-*", "-")
        val exception = validateFormat(patterns)
        assertNotNull("Expected validation error for empty exclusion pattern", exception)
        assertTrue(exception is IndexManagementException)
        assertTrue(exception!!.message!!.contains("must have content after '-' exclusion prefix"))
    }

    fun `test validateFormat with only exclusion patterns`() {
        val patterns = listOf("-log-test-*", "-log-debug-*")
        val exception = validateFormat(patterns)
        assertNotNull("Expected validation error for only exclusion patterns", exception)
        assertTrue(exception is IndexManagementException)
        assertTrue(exception!!.message!!.contains("must contain at least one inclusion pattern"))
    }

    fun `test validateFormat with valid inclusion and exclusion patterns`() {
        val patterns = listOf("log-*", "-log-test-*", "-log-*-debug-*")
        val exception = validateFormat(patterns)
        assertNull("Expected no validation error for valid patterns", exception)
    }

    fun `test validateFormat with valid inclusion patterns only`() {
        val patterns = listOf("log-*", "app-*")
        val exception = validateFormat(patterns)
        assertNull("Expected no validation error for valid inclusion patterns", exception)
    }

    fun `test validateFormat with empty string pattern`() {
        val patterns = listOf("")
        val exception = validateFormat(patterns)
        // Empty string is treated as an inclusion pattern, so it should not fail the "only exclusions" check
        // It may fail other validations depending on Strings.validFileNameExcludingAstrix
        // For now, we're just testing that it doesn't fail the exclusion-only check
        if (exception != null) {
            assertFalse(exception.message!!.contains("must contain at least one inclusion pattern"))
        }
    }
}
