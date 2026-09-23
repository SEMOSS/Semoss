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
package prerna.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class EmailUtilityUnitTests {

	private static final String[] TO = { "to@example.com" };
	private static final String[] CC = { "cc@example.com" };
	private static final String[] BCC = { "bcc@example.com" };
	private static final String[] ATTACHMENTS = { "report.txt" };
	private static final EmailUtility.EmailMetadata METADATA = new EmailUtility.EmailMetadata(TO, CC, BCC,
			"from@example.com", "subject", "body", true, ATTACHMENTS);

	@Test
	void providerDeliveryReturnsItsResult() throws Exception {
		String result = EmailUtility.sendEmail(() -> "provider-result", METADATA);

		assertEquals("provider-result", result);
	}

	@Test
	void metadataDefensivelyCopiesArrays() {
		String original = METADATA.toRecipients()[0];
		TO[0] = "changed@example.com";

		assertEquals(original, METADATA.toRecipients()[0]);
		TO[0] = original;
	}

	@Test
	void providerDeliveryRethrowsProviderFailure() {
		Exception failure = new Exception("provider failed");
		Exception thrown = assertThrows(Exception.class,
				() -> EmailUtility.sendEmail(() -> {
					throw failure;
				}, METADATA));

		assertSame(failure, thrown);
	}
}
