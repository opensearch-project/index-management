/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.util

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.ExistsQueryBuilder
import org.opensearch.index.query.TermsQueryBuilder
import org.opensearch.rest.RestStatus

@Suppress("ReturnCount", "UtilityClassWithPublicConstructor")
class SecurityUtils {
    companion object {
        const val INTERNAL_REQUEST = "index_management_plugin_internal_user"
        const val ADMIN_ROLE = "all_access"
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

        fun validateUserConfiguration(user: User?, filterEnabled: Boolean) {
            if (filterEnabled) {
                if (user == null) {
                    throw IndexManagementException.wrap(
                        OpenSearchStatusException(
                            "Filter by user backend roles in IndexManagement is not supported with security disabled",
                            RestStatus.FORBIDDEN
                        )
                    )
                } else if (user.backendRoles.isEmpty()) {
                    throw IndexManagementException.wrap(
                        OpenSearchStatusException("User doesn't have backend roles configured. Contact administrator", RestStatus.FORBIDDEN)
                    )
                }
            }
        }

        /**
         * If filterBy is enabled and security is disabled or if filter by is enabled and backend role are empty
         * we should prevent users from scheduling new jobs
         */
        fun <T> validateUserConfiguration(user: User?, filterEnabled: Boolean, actionListener: ActionListener<T>): Boolean {
            if (filterEnabled) {
                if (user == null) {
                    actionListener.onFailure(
                        IndexManagementException.wrap(
                            OpenSearchStatusException(
                                "Filter by user backend roles in IndexManagement is not supported with security disabled",
                                RestStatus.FORBIDDEN
                            )
                        )
                    )
                    return false
                } else if (user.backendRoles.isEmpty()) {
                    actionListener.onFailure(
                        IndexManagementException.wrap(
                            OpenSearchStatusException("User doesn't have backend roles configured. Contact administrator", RestStatus.FORBIDDEN)
                        )
                    )
                    return false
                }
            }
            return true
        }

        /**
         * Check if the requested user has permission on the resource
         */
        @Suppress("LongParameterList")
        fun <T> userHasPermissionForResource(
            requestedUser: User?,
            resourceUser: User?,
            filterEnabled: Boolean = false,
            resourceName: String,
            resourceId: String,
            actionListener: ActionListener<T>
        ): Boolean {
            if (!userHasPermissionForResource(requestedUser, resourceUser, filterEnabled)) {
                actionListener.onFailure(
                    IndexManagementException.wrap(
                        OpenSearchStatusException("Do not have permission for $resourceName [$resourceId]", RestStatus.FORBIDDEN)
                    )
                )
                return false
            }

            return true
        }

        /**
         * Check if the requested user has permission on the resource, throwing an exception if the user does not.
         */
        fun verifyUserHasPermissionForResource(
            requestedUser: User?,
            resourceUser: User?,
            filterEnabled: Boolean = false,
            resourceName: String,
            resourceId: String
        ) {
            if (!userHasPermissionForResource(requestedUser, resourceUser, filterEnabled)) {
                throw IndexManagementException.wrap(
                    OpenSearchStatusException("Do not have permission for $resourceName [$resourceId]", RestStatus.FORBIDDEN)
                )
            }
        }

        /**
         * Check if the requested user has permission on the resource
         */
        @Suppress("ComplexCondition")
        fun userHasPermissionForResource(
            requestedUser: User?,
            resourceUser: User?,
            filterEnabled: Boolean = false
        ): Boolean {
            // Will not filter if filter is not enabled or stored user is null or requested user is null or if the user is admin
            if (!filterEnabled || resourceUser == null || requestedUser == null || requestedUser.roles.contains(ADMIN_ROLE)) {
                return true
            }

            val resourceBackendRoles = resourceUser.backendRoles
            val requestedBackendRoles = requestedUser.backendRoles

            return !(resourceBackendRoles == null || requestedBackendRoles == null || resourceBackendRoles.intersect(requestedBackendRoles).isEmpty())
        }

        /**
         * Add user filter to search requests
         */
        fun addUserFilter(user: User?, queryBuilder: BoolQueryBuilder, filterEnabled: Boolean = false, filterPathPrefix: String) {
            if (!filterEnabled || user == null || user.roles.contains(ADMIN_ROLE)) {
                return
            }

            val filterQuery = BoolQueryBuilder().should(
                TermsQueryBuilder("$filterPathPrefix.backend_roles.keyword", user.backendRoles)
            ).should(
                BoolQueryBuilder().mustNot(
                    ExistsQueryBuilder(filterPathPrefix)
                )
            )
            queryBuilder.filter(filterQuery)
        }

        /**
         * Generates a user string formed by the username, backend roles, roles and requested tenants separated by '|'. This is the user
         * string format used internally in the OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT and may be parsed using User.parse(<user string>).
         */
        fun generateUserString(user: User?): String {
            if (user == null) return ""
            val backendRoles = user.backendRoles.joinToString(",")
            val roles = user.roles.joinToString(",")
            val requestedTenant = user.requestedTenant
            val userName = user.name
            return "$userName|$backendRoles|$roles|$requestedTenant"
        }
    }
}
