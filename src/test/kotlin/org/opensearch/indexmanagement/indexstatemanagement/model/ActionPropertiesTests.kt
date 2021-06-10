/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import org.opensearch.test.OpenSearchTestCase

class ActionPropertiesTests : OpenSearchTestCase() {

    @Suppress("UNCHECKED_CAST")
    fun `test action properties exist in history index`() {
        // All properties inside the ActionProperties class need to be also added to the ism history mappings
        // This is to catch any commits/PRs that add to ActionProperties but forget to add to history mappings
        val expected = createParser(
            XContentType.JSON.xContent(),
            javaClass.classLoader.getResource("mappings/opendistro-ism-history.json")!!.readText()
        )
        val expectedMap = expected.map() as Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Any>>>>>>>>
        val actionProperties = ActionProperties.Properties.values().map { it.key }
        val mappingActionProperties = expectedMap["properties"]!!["managed_index_meta_data"]!!["properties"]!!["action"]!!["properties"]!!["action_properties"]!!["properties"]
        assertNotNull("Could not get action properties from ism history mappings", mappingActionProperties)
        actionProperties.forEach { property ->
            assertTrue("The $property action property does not exist in the ism history mappings", mappingActionProperties!!.containsKey(property))
        }
    }
}
