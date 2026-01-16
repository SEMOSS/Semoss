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
 * Store for MCP access tokens issued to ChatGPT/Claude.
 * Tokens are short-lived (1 hour) and used to authenticate MCP requests.
 */
public class MCPTokenStore {
    private static MCPTokenStore instance;
    private Map<String, TokenData> tokens = new ConcurrentHashMap<>();
    private static final long TOKEN_EXPIRATION_MS = 60 * 60 * 1000; // 1 hour

    public static synchronized MCPTokenStore getInstance() {
        if (instance == null) {
            instance = new MCPTokenStore();
        }
        return instance;
    }

    public void storeToken(String token, User user) {
        long expiresAt = System.currentTimeMillis() + TOKEN_EXPIRATION_MS;
        tokens.put(token, new TokenData(user, expiresAt));
    }

    public User validateToken(String token) {
        TokenData data = tokens.get(token);
        if (data != null && !data.isExpired()) {
            return data.getUser();
        }
        tokens.remove(token); // Clean up expired token
        return null;
    }

    public void revokeToken(String token) {
        tokens.remove(token);
    }

    private static class TokenData {
        private User user;
        private long expiresAt;

        public TokenData(User user, long expiresAt) {
            this.user = user;
            this.expiresAt = expiresAt;
        }

        public User getUser() {
            return user;
        }

        public boolean isExpired() {
            return System.currentTimeMillis() > expiresAt;
        }
    }
}
