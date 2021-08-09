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

package org.opensearch.indexmanagement.util

import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User

class SecurityUtils {
    companion object {
        const val INTERNAL_REQUEST = "index_management_plugin_internal_user"
        val DEFAULT_INJECT_ROLES: List<String> = listOf("all_access", "AmazonES_all_access")
        /**
         * Helper method to build the user object either from the threadContext or from the requested user.
         */
        fun buildUser(threadContext: ThreadContext, requestedUser: User? = null): User? {
            if (threadContext.getTransient<Boolean>(INTERNAL_REQUEST) != null && threadContext.getTransient<Boolean>(INTERNAL_REQUEST)) {
                // received internal request
                return requestedUser
            }

            val injectedUser: User? = User.parse(threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT))
            return if (injectedUser == null) {
                null
            } else {
                User(injectedUser.name, injectedUser.backendRoles, injectedUser.roles, injectedUser.customAttNames)
            }
        }
    }
}
