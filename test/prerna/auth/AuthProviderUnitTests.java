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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class AuthProviderUnitTests {

	@ParameterizedTest
	@ValueSource(strings = { "ms", "MS", "Ms", "mS", "microsoft", "Microsoft", "MICROSOFT", " MS " })
	void microsoftAliasesResolveToTheSameProviderAndStoredLabel(String alias) {
		assertEquals(AuthProvider.MICROSOFT, AuthProvider.getProviderFromString(alias));
		assertEquals("MICROSOFT", AuthProvider.getProviderLabel(alias));
		assertEquals("ms", AuthProvider.getSocialPrefixForPath(alias));
	}

	@ParameterizedTest
	@EnumSource(AuthProvider.class)
	void allProviderNamesAndConfigurationPrefixesShareTheSameMapping(AuthProvider provider) {
		for (String value : new String[] { provider.name(), provider.getSocialPrefix() }) {
			assertEquals(provider, AuthProvider.getProviderFromString(value));
			assertEquals(provider.getLabel(), AuthProvider.getProviderLabel(value));
			assertEquals(provider.getSocialPrefix(), AuthProvider.getSocialPrefixForPath(value));
			assertTrue(AuthProvider.getSocialPropKeys().contains(value.toLowerCase(Locale.ROOT)));
		}
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "CUSTOM", "custom", "custom-realm", "https://identity.example/realm", "MS_TEAM", " " })
	void unknownGroupNamespacesArePreservedInsteadOfBecomingGeneric(String value) {
		assertEquals(value, AuthProvider.getProviderLabel(value));
		assertEquals(AuthProvider.GENERIC, AuthProvider.getProviderFromString(value));
	}

	@Test
	void providerMatchingIsIndependentOfTheDefaultLocale() {
		Locale previous = Locale.getDefault();
		try {
			Locale.setDefault(Locale.forLanguageTag("tr-TR"));
			assertEquals(AuthProvider.MICROSOFT, AuthProvider.getProviderFromString("MICROSOFT"));
			assertEquals("MICROSOFT", AuthProvider.getProviderLabel("microsoft"));
			assertEquals("ms", AuthProvider.getSocialPrefixForPath("MICROSOFT"));
		} finally {
			Locale.setDefault(previous);
		}
	}

	@Test
	void unknownLoginPrefixesKeepTheirExistingFallback() {
		assertEquals("custom-realm", AuthProvider.getSocialPrefixForPath("Custom-Realm"));
		assertNull(AuthProvider.getSocialPrefixForPath(null));
	}
}
