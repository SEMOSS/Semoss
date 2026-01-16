/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth.mcp;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import prerna.auth.User;

/**
 * Store for OAuth authorization codes during the MCP authentication flow.
 * Codes are one-time use and expire after 10 minutes.
 */
public class MCPAuthorizationCodeStore {
    private static MCPAuthorizationCodeStore instance;
    private Map<String, AuthorizationCode> codes = new ConcurrentHashMap<>();
    private static final long CODE_EXPIRATION_MS = 10 * 60 * 1000; // 10 minutes

    public static synchronized MCPAuthorizationCodeStore getInstance() {
        if (instance == null) {
            instance = new MCPAuthorizationCodeStore();
        }
        return instance;
    }

    public void storeCode(String code, User user, Map<String, String> authRequest) {
        long expiresAt = System.currentTimeMillis() + CODE_EXPIRATION_MS;
        codes.put(code, new AuthorizationCode(code, user, authRequest, expiresAt));
    }

    public AuthorizationCode consumeCode(String code) {
        AuthorizationCode authCode = codes.remove(code); // One-time use
        if (authCode != null && authCode.isExpired()) {
            return null;
        }
        return authCode;
    }

    public static class AuthorizationCode {
        private String code;
        private User user;
        private Map<String, String> authRequest;
        private long expiresAt;

        public AuthorizationCode(String code, User user, Map<String, String> authRequest, long expiresAt) {
            this.code = code;
            this.user = user;
            this.authRequest = authRequest;
            this.expiresAt = expiresAt;
        }

        public String getCode() {
            return code;
        }

        public User getUser() {
            return user;
        }

        public Map<String, String> getAuthRequest() {
            return authRequest;
        }

        public String getCodeChallenge() {
            return authRequest.get("code_challenge");
        }

        public boolean isExpired() {
            return System.currentTimeMillis() > expiresAt;
        }
    }
}
