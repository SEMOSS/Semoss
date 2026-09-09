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
package prerna.engine.impl.function.mail.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.function.mail.config.MailProperties;
import prerna.engine.impl.function.mail.model.OutboundMail;

class MailPoliciesUnitTests {

	@Test
	void sendPolicyAppliesDefaultsAndGuardrailsBeforeDelivery() {
		Properties properties = new Properties();
		properties.setProperty(MailProperties.SMTP_SENDER, "sender@example.com");
		properties.setProperty(MailProperties.DEFAULT_TO, "default@example.com");
		properties.setProperty(MailProperties.ALLOWED_RECIPIENT_DOMAINS, "example.com");
		properties.setProperty(MailProperties.SUBJECT_PREFIX, "[SEMOSS]");

		OutboundMail message = SendMailPolicy.from(properties)
				.prepare(Map.of("subject", "Report", "message", "Ready"), null);

		assertEquals("sender@example.com", message.from());
		assertEquals(java.util.List.of("default@example.com"), message.to());
		assertEquals("[SEMOSS] Report", message.subject());
		assertEquals("Ready", message.body());
	}

	@Test
	void sendPolicyRejectsRecipientsOutsideConfiguredDomains() {
		Properties properties = new Properties();
		properties.setProperty(MailProperties.SMTP_SENDER, "sender@example.com");
		properties.setProperty(MailProperties.ALLOWED_RECIPIENT_DOMAINS, "example.com");
		SendMailPolicy policy = SendMailPolicy.from(properties);

		assertThrows(IllegalArgumentException.class, () -> policy.prepare(
				Map.of("to", "outside@other.com", "subject", "Report", "message", "Ready"), null));
	}

	@Test
	void readPolicyBoundsResultsAndAllowsSubdomains() {
		Properties properties = new Properties();
		properties.setProperty(MailProperties.MAX_MESSAGES, "5");
		properties.setProperty(MailProperties.DEFAULT_MESSAGES, "20");
		properties.setProperty(MailProperties.ALLOWED_SENDER_DOMAINS, "@example.com");
		MailReadPolicy policy = MailReadPolicy.from(properties);

		assertEquals(5, policy.defaultMessages());
		assertEquals(1, policy.boundedLimit(0));
		assertEquals(5, policy.boundedLimit(100));
		assertTrue(policy.isSenderAllowed("Person <sender@reports.example.com>"));
		assertFalse(policy.isSenderAllowed("sender@other.com"));
	}
}
