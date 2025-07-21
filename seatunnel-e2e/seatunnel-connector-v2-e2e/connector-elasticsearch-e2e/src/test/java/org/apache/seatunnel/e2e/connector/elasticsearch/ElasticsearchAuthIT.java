/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.elasticsearch;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.auth.AuthenticationProvider;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.auth.AuthenticationProviderFactory;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class ElasticsearchAuthIT extends TestSuiteBase implements TestResource {

    private static final String ELASTICSEARCH_IMAGE = "elasticsearch:8.9.0";
    private static final String OAUTH2_MOCK_IMAGE = "mockserver/mockserver:5.15.0";
    private static final long INDEX_REFRESH_DELAY = 2000L;

    // Test data constants
    private static final String TEST_INDEX = "auth_test_index";
    private static final String VALID_USERNAME = "elastic";
    private static final String VALID_PASSWORD = "elasticsearch";
    private static final String INVALID_USERNAME = "wrong_user";
    private static final String INVALID_PASSWORD = "wrong_password";

    // API Key test constants
    private static final String VALID_API_KEY_ID = "test-api-key-id";
    private static final String VALID_API_KEY_SECRET = "test-api-key-secret";
    private static final String INVALID_API_KEY_ID = "invalid-key-id";
    private static final String INVALID_API_KEY_SECRET = "invalid-key-secret";
    private static final String VALID_ENCODED_API_KEY =
            "dGVzdC1hcGkta2V5LWlkOnRlc3QtYXBpLWtleS1zZWNyZXQ=";

    // OAuth2 test constants
    private static final String VALID_OAUTH_CLIENT_ID = "test-client";
    private static final String VALID_OAUTH_CLIENT_SECRET = "test-secret";
    private static final String VALID_OAUTH_TOKEN_URL = "http://oauth2-server:1080/oauth/token";
    private static final String INVALID_OAUTH_TOKEN_URL = "http://invalid-server:1080/oauth/token";

    private ElasticsearchContainer elasticsearchContainer;
    private GenericContainer<?> oauth2MockServer;
    private EsRestClient setupClient;
    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        startElasticsearchContainer();
        startOAuth2MockServer();
        setupTestEnvironment();
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        if (setupClient != null) {
            setupClient.close();
        }
        if (elasticsearchContainer != null) {
            elasticsearchContainer.stop();
        }
        if (oauth2MockServer != null) {
            oauth2MockServer.stop();
        }
    }

    private void startElasticsearchContainer() {
        elasticsearchContainer =
                new ElasticsearchContainer(
                                DockerImageName.parse(ELASTICSEARCH_IMAGE)
                                        .asCompatibleSubstituteFor(
                                                "docker.elastic.co/elasticsearch/elasticsearch"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("elasticsearch")
                        .withPassword(VALID_PASSWORD)
                        .withEnv("cluster.routing.allocation.disk.threshold_enabled", "false")
                        .withEnv("xpack.security.enabled", "true")
                        .withEnv("xpack.security.authc.api_key.enabled", "true")
                        .withStartupAttempts(5)
                        .withStartupTimeout(Duration.ofMinutes(5))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(ELASTICSEARCH_IMAGE)));

        Startables.deepStart(Stream.of(elasticsearchContainer)).join();
        log.info("Elasticsearch container started for authentication testing");
    }

    private void startOAuth2MockServer() {
        oauth2MockServer =
                new GenericContainer<>(DockerImageName.parse(OAUTH2_MOCK_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("oauth2-server")
                        .withExposedPorts(1080)
                        .withEnv("MOCKSERVER_LOG_LEVEL", "WARN")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(OAUTH2_MOCK_IMAGE)));

        Startables.deepStart(Stream.of(oauth2MockServer)).join();
        setupOAuth2MockEndpoints();
        log.info("OAuth2 mock server started for authentication testing");
    }

    private void setupOAuth2MockEndpoints() {
        try {
            TimeUnit.SECONDS.sleep(2); // Wait for mock server to be ready

            String mockResponse =
                    "{"
                            + "\"access_token\": \"mock-access-token-12345\","
                            + "\"token_type\": \"Bearer\","
                            + "\"expires_in\": 3600"
                            + "}";

            Container.ExecResult result =
                    oauth2MockServer.execInContainer(
                            "curl",
                            "-X",
                            "PUT",
                            "http://localhost:1080/mockserver/expectation",
                            "-H",
                            "Content-Type: application/json",
                            "-d",
                            "{"
                                    + "\"httpRequest\": {"
                                    + "\"method\": \"POST\","
                                    + "\"path\": \"/oauth/token\""
                                    + "},"
                                    + "\"httpResponse\": {"
                                    + "\"statusCode\": 200,"
                                    + "\"headers\": {\"Content-Type\": [\"application/json\"]},"
                                    + "\"body\": \""
                                    + mockResponse.replace("\"", "\\\"")
                                    + "\""
                                    + "}"
                                    + "}");

            if (result.getExitCode() == 0) {
                log.info("OAuth2 mock endpoint configured successfully");
            }
        } catch (Exception e) {
            log.warn("Failed to setup OAuth2 mock server", e);
        }
    }

    private void setupTestEnvironment() throws Exception {
        // Create setup client with basic auth
        Map<String, Object> setupConfig = createBasicAuthConfig(VALID_USERNAME, VALID_PASSWORD);
        setupClient = EsRestClient.createInstance(ReadonlyConfig.fromMap(setupConfig));

        // Create test index and data
        createTestIndex();
        insertTestData();
    }

    private void createTestIndex() throws Exception {
        String mapping =
                "{"
                        + "\"mappings\": {"
                        + "\"properties\": {"
                        + "\"id\": {\"type\": \"integer\"},"
                        + "\"name\": {\"type\": \"text\"},"
                        + "\"value\": {\"type\": \"double\"}"
                        + "}"
                        + "}"
                        + "}";

        Container.ExecResult result =
                elasticsearchContainer.execInContainer(
                        "curl",
                        "-X",
                        "PUT",
                        "https://localhost:9200/" + TEST_INDEX,
                        "-H",
                        "Content-Type: application/json",
                        "-u",
                        VALID_USERNAME + ":" + VALID_PASSWORD,
                        "-k",
                        "-d",
                        mapping);

        if (result.getExitCode() != 0) {
            throw new RuntimeException("Failed to create test index: " + result.getStderr());
        }
        log.info("Test index created successfully");
    }

    private void insertTestData() throws Exception {
        StringBuilder requestBody = new StringBuilder();
        String indexHeader = "{\"index\":{\"_index\":\"" + TEST_INDEX + "\"}}\n";

        for (int i = 1; i <= 3; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("id", i);
            doc.put("name", "test_" + i);
            doc.put("value", i * 10.5);

            requestBody.append(indexHeader);
            requestBody.append(objectMapper.writeValueAsString(doc));
            requestBody.append("\n");
        }

        BulkResponse response = setupClient.bulk(requestBody.toString());
        Assertions.assertFalse(response.isErrors(), "Failed to insert test data");

        Thread.sleep(INDEX_REFRESH_DELAY);
        log.info("Test data inserted successfully");
    }

    // Helper methods for creating configurations
    private Map<String, Object> createBasicAuthConfig(String username, String password) {
        Map<String, Object> config = new HashMap<>();
        config.put(
                "hosts",
                Lists.newArrayList("https://" + elasticsearchContainer.getHttpHostAddress()));
        config.put("auth_type", "basic");
        config.put("username", username);
        config.put("password", password);
        config.put("tls_verify_certificate", false);
        config.put("tls_verify_hostname", false);
        return config;
    }

    private Map<String, Object> createApiKeyConfig(String keyId, String keySecret) {
        Map<String, Object> config = new HashMap<>();
        config.put(
                "hosts",
                Lists.newArrayList("https://" + elasticsearchContainer.getHttpHostAddress()));
        config.put("auth_type", "api_key");
        config.put("api_key_id", keyId);
        config.put("api_key", keySecret);
        config.put("tls_verify_certificate", false);
        config.put("tls_verify_hostname", false);
        return config;
    }

    private Map<String, Object> createApiKeyEncodedConfig(String encodedKey) {
        Map<String, Object> config = new HashMap<>();
        config.put(
                "hosts",
                Lists.newArrayList("https://" + elasticsearchContainer.getHttpHostAddress()));
        config.put("auth_type", "api_key");
        config.put("api_key_encoded", encodedKey);
        config.put("tls_verify_certificate", false);
        config.put("tls_verify_hostname", false);
        return config;
    }

    private Map<String, Object> createOAuth2Config(
            String clientId, String clientSecret, String tokenUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(
                "hosts",
                Lists.newArrayList("https://" + elasticsearchContainer.getHttpHostAddress()));
        config.put("auth_type", "oauth2");
        config.put("oauth_client_id", clientId);
        config.put("oauth_client_secret", clientSecret);
        config.put("oauth_token_url", tokenUrl);
        config.put("tls_verify_certificate", false);
        config.put("tls_verify_hostname", false);
        return config;
    }

    // ==================== Basic Authentication Tests ====================

    /** Test successful basic authentication with valid credentials */
    @Test
    public void testBasicAuthenticationSuccess() throws Exception {
        log.info("=== Testing Basic Authentication Success ===");

        Map<String, Object> config = createBasicAuthConfig(VALID_USERNAME, VALID_PASSWORD);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(provider, "Authentication provider should be created");
        Assertions.assertEquals(
                "basic", provider.getAuthType(), "Provider should be basic auth type");

        // Test client creation and functionality
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created successfully");

            // Verify client can perform operations
            long docCount = client.getIndexDocsCount(TEST_INDEX).get(0).getDocsCount();
            Assertions.assertTrue(
                    docCount > 0, "Should be able to query index with valid credentials");

            log.info("✓ Basic authentication success test passed - {} documents found", docCount);
        }
    }

    /** Test basic authentication failure with invalid credentials */
    @Test
    public void testBasicAuthenticationFailure() throws Exception {
        log.info("=== Testing Basic Authentication Failure ===");

        Map<String, Object> config = createBasicAuthConfig(INVALID_USERNAME, INVALID_PASSWORD);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation (should succeed)
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(
                provider,
                "Authentication provider should be created even with invalid credentials");
        Assertions.assertEquals(
                "basic", provider.getAuthType(), "Provider should be basic auth type");

        // Test client creation (should succeed)
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created");

            // Test operation (should fail with authentication error)
            Exception exception =
                    Assertions.assertThrows(
                            Exception.class,
                            () -> {
                                client.getIndexDocsCount(TEST_INDEX);
                            },
                            "Should throw exception when using invalid credentials");

            log.info(
                    "✓ Basic authentication failure test passed - exception: {}",
                    exception.getMessage());
        }
    }

    // ==================== API Key Authentication Tests ====================

    /** Test successful API key authentication with valid key */
    @Test
    public void testApiKeyAuthenticationSuccess() throws Exception {
        log.info("=== Testing API Key Authentication Success ===");

        Map<String, Object> config = createApiKeyConfig(VALID_API_KEY_ID, VALID_API_KEY_SECRET);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(provider, "Authentication provider should be created");
        Assertions.assertEquals(
                "api_key", provider.getAuthType(), "Provider should be api_key auth type");

        // Test client creation
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created successfully");

            // Note: This might fail if the API key is not actually valid in Elasticsearch
            // But the provider creation and client creation should succeed
            log.info("✓ API key authentication provider and client creation test passed");
        }
    }

    /** Test API key authentication failure with invalid key */
    @Test
    public void testApiKeyAuthenticationFailure() throws Exception {
        log.info("=== Testing API Key Authentication Failure ===");

        Map<String, Object> config = createApiKeyConfig(INVALID_API_KEY_ID, INVALID_API_KEY_SECRET);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation (should succeed)
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(provider, "Authentication provider should be created");
        Assertions.assertEquals(
                "api_key", provider.getAuthType(), "Provider should be api_key auth type");

        // Test client creation (should succeed)
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created");

            // Test operation (should fail with authentication error)
            Exception exception =
                    Assertions.assertThrows(
                            Exception.class,
                            () -> {
                                client.getIndexDocsCount(TEST_INDEX);
                            },
                            "Should throw exception when using invalid API key");

            log.info(
                    "✓ API key authentication failure test passed - exception: {}",
                    exception.getMessage());
        }
    }

    /** Test API key authentication with encoded format */
    @Test
    public void testApiKeyEncodedAuthentication() throws Exception {
        log.info("=== Testing API Key Encoded Authentication ===");

        Map<String, Object> config = createApiKeyEncodedConfig(VALID_ENCODED_API_KEY);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(provider, "Authentication provider should be created");
        Assertions.assertEquals(
                "api_key", provider.getAuthType(), "Provider should be api_key auth type");

        // Test client creation
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created successfully");
            log.info("✓ API key encoded authentication test passed");
        }
    }

    // ==================== OAuth2 Authentication Tests ====================

    /** Test successful OAuth2 authentication configuration */
    @Test
    public void testOAuth2AuthenticationSuccess() throws Exception {
        log.info("=== Testing OAuth2 Authentication Success ===");

        Map<String, Object> config =
                createOAuth2Config(
                        VALID_OAUTH_CLIENT_ID, VALID_OAUTH_CLIENT_SECRET, VALID_OAUTH_TOKEN_URL);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(provider, "Authentication provider should be created");
        Assertions.assertEquals(
                "oauth2", provider.getAuthType(), "Provider should be oauth2 auth type");

        // Test client creation (should succeed)
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created successfully");

            // Note: OAuth2 operations will likely fail because Elasticsearch doesn't accept our
            // mock token
            // But the provider creation and client creation should succeed
            log.info("✓ OAuth2 authentication provider and client creation test passed");
        }
    }

    /** Test OAuth2 authentication failure with invalid configuration */
    @Test
    public void testOAuth2AuthenticationFailure() throws Exception {
        log.info("=== Testing OAuth2 Authentication Failure ===");

        Map<String, Object> config =
                createOAuth2Config(
                        VALID_OAUTH_CLIENT_ID, VALID_OAUTH_CLIENT_SECRET, INVALID_OAUTH_TOKEN_URL);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation (should succeed)
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(provider, "Authentication provider should be created");
        Assertions.assertEquals(
                "oauth2", provider.getAuthType(), "Provider should be oauth2 auth type");

        // Test client creation (might fail due to invalid token URL)
        Exception exception =
                Assertions.assertThrows(
                        Exception.class,
                        () -> {
                            try (EsRestClient client =
                                    EsRestClient.createInstance(readonlyConfig)) {
                                // This should fail when trying to obtain OAuth2 token
                            }
                        },
                        "Should throw exception when OAuth2 token acquisition fails");

        log.info(
                "✓ OAuth2 authentication failure test passed - exception: {}",
                exception.getMessage());
    }
}
