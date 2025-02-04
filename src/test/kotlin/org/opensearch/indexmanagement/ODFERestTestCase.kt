/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.apache.hc.core5.http.HttpHost
import org.opensearch.client.RestClient
import org.opensearch.common.io.PathUtils
import org.opensearch.common.settings.MockSecureSettings
import org.opensearch.common.settings.SecureSettings
import org.opensearch.common.settings.Settings
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_ENABLED
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_FILEPATH
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_KEYPASSWORD_SETTING
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_PASSWORD_SETTING
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_PEMCERT_FILEPATH
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.io.IOException

abstract class ODFERestTestCase : OpenSearchRestTestCase() {
    fun isHttps(): Boolean = System.getProperty("https", "false")!!.toBoolean()

    fun securityEnabled(): Boolean = System.getProperty("security", "false")!!.toBoolean()

    override fun getProtocol(): String = if (isHttps()) "https" else "http"

    /**
     * Returns the REST client settings used for super-admin actions like cleaning up after the test has completed.
     */
    protected open fun createSecureSettings(): SecureSettings? {
        val mockSecureSettings = MockSecureSettings()
        mockSecureSettings.setString(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_PASSWORD_SETTING.key, "changeit")
        mockSecureSettings.setString(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_KEYPASSWORD_SETTING.key, "changeit")
        return mockSecureSettings
    }

    override fun restAdminSettings(): Settings = Settings
        .builder()
        .put("http.port", 9200)
        .put(OPENSEARCH_SECURITY_SSL_HTTP_ENABLED, isHttps())
        .put(OPENSEARCH_SECURITY_SSL_HTTP_PEMCERT_FILEPATH, "sample.pem")
        .put(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_FILEPATH, "test-kirk.jks")
        .setSecureSettings(createSecureSettings())
        .build()

    @Throws(IOException::class)
    override fun buildClient(settings: Settings, hosts: Array<HttpHost>): RestClient {
        if (isHttps()) {
            val keystore = settings.get(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_FILEPATH)
            return when (keystore != null) {
                true -> {
                    // create adminDN (super-admin) client
                    val uri = javaClass.classLoader.getResource("sample.pem")?.toURI()
                    val configPath = PathUtils.get(uri).parent.toAbsolutePath()
                    SecureRestClientBuilder(settings, configPath, hosts).setSocketTimeout(60000)
                        .setConnectionRequestTimeout(180000).build()
                }

                false -> {
                    // create client with passed user
                    val userName = System.getProperty("user")
                    val password = System.getProperty("password")
                    SecureRestClientBuilder(hosts, isHttps(), userName, password).setSocketTimeout(60000)
                        .setConnectionRequestTimeout(180000).build()
                }
            }
        } else {
            val builder = RestClient.builder(*hosts)
            configureClient(builder, settings)
            builder.setStrictDeprecationMode(true)
            return builder.build()
        }
    }
}
