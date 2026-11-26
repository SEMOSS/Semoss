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
package prerna.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class AccessTokenUnitTests {

	@Test
	void theRefreshTokenIsReturnedOnlyWithTheAccessTokenAndNeverInTheMetadata() {
		AccessToken token = new AccessToken();
		token.setId("user-one");
		token.setAccess_token("access");
		token.setRefresh_token("refresh");
		token.addMetaValue("text-generation-model", "model-one");

		Map<String, Object> withoutToken = token.toProviderMap(false, "epoch", false);
		assertFalse(withoutToken.containsKey("accessToken"));
		assertFalse(withoutToken.containsKey("refreshToken"));
		assertEquals(Map.of("text-generation-model", List.of("model-one")), withoutToken.get("meta"));

		Map<String, Object> withToken = token.toProviderMap(true, "epoch", false);
		assertEquals("access", withToken.get("accessToken"));
		assertEquals("refresh", withToken.get("refreshToken"));
		assertEquals(Map.of("text-generation-model", List.of("model-one")), withToken.get("meta"));
	}

	@Test
	void copiesKeepTheRefreshTokenAndAReadOnlyCopyRefusesToChangeIt() {
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.MICROSOFT);
		token.setRefresh_token("refresh");

		assertEquals("refresh", AccessToken.copyToken(token).getRefresh_token());
		AccessToken readOnly = ReadOnlyAccessToken.unmodifiableToken(token);
		assertEquals("refresh", readOnly.getRefresh_token());
		assertThrows(IllegalArgumentException.class, () -> readOnly.setRefresh_token("other"));
	}
}
