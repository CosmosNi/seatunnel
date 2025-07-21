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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.auth;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class OAuth2Provider extends AbstractAuthenticationProvider {

    private static final String AUTH_TYPE = "oauth2";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String GRANT_TYPE = "client_credentials";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final AtomicReference<String> currentToken = new AtomicReference<>();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "oauth2-token-refresh");
                        t.setDaemon(true);
                        return t;
                    });

    @Override
    protected void configureAuthentication(
            HttpAsyncClientBuilder httpClientBuilder, ReadonlyConfig config) {
        try {
            // Get initial token
            String token = obtainAccessToken(config);
            currentToken.set(token);

            // Configure the HTTP client with a dynamic header provider
            httpClientBuilder.addInterceptorFirst(
                    (org.apache.http.HttpRequestInterceptor)
                            (request, context) -> {
                                String currentAccessToken = currentToken.get();
                                if (currentAccessToken != null) {
                                    request.setHeader(
                                            AUTHORIZATION_HEADER,
                                            BEARER_PREFIX + currentAccessToken);
                                }
                            });

            // Schedule token refresh (refresh every 50 minutes for 1-hour tokens)
            scheduler.scheduleAtFixedRate(
                    () -> {
                        try {
                            String newToken = obtainAccessToken(config);
                            currentToken.set(newToken);
                            log.debug("OAuth2 token refreshed successfully");
                        } catch (Exception e) {
                            log.error("Failed to refresh OAuth2 token", e);
                        }
                    },
                    50,
                    50,
                    TimeUnit.MINUTES);

            log.info("OAuth2 authentication configured successfully");
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure OAuth2 authentication", e);
        }
    }

    @Override
    public String getAuthType() {
        return AUTH_TYPE;
    }

    @Override
    public void validate(ReadonlyConfig config) {
        Optional<String> clientId = config.getOptional(ElasticsearchBaseOptions.OAUTH_CLIENT_ID);
        Optional<String> clientSecret =
                config.getOptional(ElasticsearchBaseOptions.OAUTH_CLIENT_SECRET);
        Optional<String> tokenUrl = config.getOptional(ElasticsearchBaseOptions.OAUTH_TOKEN_URL);

        if (!clientId.isPresent()) {
            throw new IllegalArgumentException("OAuth2 client ID is required");
        }

        if (!clientSecret.isPresent()) {
            throw new IllegalArgumentException("OAuth2 client secret is required");
        }

        if (!tokenUrl.isPresent()) {
            throw new IllegalArgumentException("OAuth2 token URL is required");
        }

        String clientIdValue = clientId.get();
        if (clientIdValue == null || clientIdValue.trim().isEmpty()) {
            throw new IllegalArgumentException("OAuth2 client ID cannot be null or empty");
        }

        String clientSecretValue = clientSecret.get();
        if (clientSecretValue == null || clientSecretValue.trim().isEmpty()) {
            throw new IllegalArgumentException("OAuth2 client secret cannot be null or empty");
        }

        String tokenUrlValue = tokenUrl.get();
        if (tokenUrlValue == null || tokenUrlValue.trim().isEmpty()) {
            throw new IllegalArgumentException("OAuth2 token URL cannot be null or empty");
        }

        // Validate URL format
        try {
            new java.net.URL(tokenUrlValue);
        } catch (java.net.MalformedURLException e) {
            throw new IllegalArgumentException(
                    "Invalid OAuth2 token URL format: " + tokenUrlValue, e);
        }

        log.debug("OAuth2 authentication configuration validated");
    }

    /** Obtain an access token using the OAuth2 Client Credentials flow. */
    private String obtainAccessToken(ReadonlyConfig config) throws IOException {
        String clientId = config.get(ElasticsearchBaseOptions.OAUTH_CLIENT_ID);
        String clientSecret = config.get(ElasticsearchBaseOptions.OAUTH_CLIENT_SECRET);
        String tokenUrl = config.get(ElasticsearchBaseOptions.OAUTH_TOKEN_URL);
        Optional<String> scope = config.getOptional(ElasticsearchBaseOptions.OAUTH_SCOPE);

        HttpClient httpClient = HttpClients.createDefault();
        HttpPost request = new HttpPost(tokenUrl);

        // Set Authorization header with client credentials
        String credentials = clientId + ":" + clientSecret;
        String encodedCredentials =
                Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        request.setHeader("Authorization", "Basic " + encodedCredentials);
        request.setHeader("Content-Type", "application/x-www-form-urlencoded");

        // Set request body
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("grant_type", GRANT_TYPE));
        if (scope.isPresent() && !scope.get().trim().isEmpty()) {
            params.add(new BasicNameValuePair("scope", scope.get()));
        }
        request.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));

        HttpResponse response = httpClient.execute(request);
        HttpEntity entity = response.getEntity();
        String responseBody = EntityUtils.toString(entity);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException(
                    "Failed to obtain OAuth2 token. Status: "
                            + response.getStatusLine().getStatusCode()
                            + ", Response: "
                            + responseBody);
        }

        try {
            JsonNode jsonResponse = OBJECT_MAPPER.readTree(responseBody);
            String accessToken = jsonResponse.get("access_token").asText();

            if (accessToken == null || accessToken.trim().isEmpty()) {
                throw new RuntimeException("Invalid OAuth2 response: missing access_token");
            }

            log.debug("Successfully obtained OAuth2 access token");
            return accessToken;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse OAuth2 token response: " + responseBody, e);
        }
    }
}
