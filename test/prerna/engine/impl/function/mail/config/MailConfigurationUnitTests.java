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
package prerna.engine.impl.function.mail.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;

import org.junit.jupiter.api.Test;

class MailConfigurationUnitTests {

	@Test
	void microsoftConfigurationUsesOnlyExchangeCredentialKeys() {
		Properties properties = new Properties();
		properties.setProperty(MailProperties.EXCHANGE_TENANT, " tenant ");
		properties.setProperty(MailProperties.EXCHANGE_CLIENT_ID, " client ");
		properties.setProperty(MailProperties.EXCHANGE_CLIENT_SECRET, " secret ");
		properties.setProperty(MailProperties.EXCHANGE_SCOPE, " scope ");

		Microsoft365Config config = Microsoft365Config.from(properties, "default-scope");

		assertEquals("tenant", config.tenant());
		assertEquals("client", config.clientId());
		assertEquals("secret", config.clientSecret());
		assertEquals("scope", config.scope());
	}

	@Test
	void legacyGraphCredentialKeysAreNotRead() {
		Properties properties = new Properties();
		properties.setProperty("GRAPH_TENANT", "tenant");
		properties.setProperty("GRAPH_CLIENT_ID", "client");
		properties.setProperty("GRAPH_CLIENT_SECRET", "secret");
		properties.setProperty("GRAPH_SCOPE", "legacy-scope");

		Microsoft365Config config = Microsoft365Config.from(properties, "default-scope");

		assertNull(config.tenant());
		assertNull(config.clientId());
		assertNull(config.clientSecret());
		assertEquals("default-scope", config.scope());
		assertThrows(IllegalArgumentException.class, config::tokenProvider);
	}

	@Test
	void rawJakartaPropertiesAreNormalizedWithoutChangingEngineKeys() {
		Properties properties = new Properties();
		properties.setProperty("MAIL.IMAPS.SSL.TRUST", "mail.example.com");
		properties.setProperty(MailProperties.SMTP_HOST, "smtp.example.com");

		Properties normalized = MailProperties.normalize(properties);

		assertEquals("mail.example.com", normalized.getProperty("mail.imaps.ssl.trust"));
		assertEquals("smtp.example.com", normalized.getProperty(MailProperties.SMTP_HOST));
	}
}
