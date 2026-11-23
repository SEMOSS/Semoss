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
package prerna.io.connector.google.gmail;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class GoogleGmailFoldersUnitTests {

	private final GoogleGmailHelper gmail = mock(GoogleGmailHelper.class);

	@Test
	void aNestedLabelResolvesByItsOwnNameTheShallowestFirst() {
		when(gmail.listLabels()).thenReturn(List.of(label("Label_3", "Clients/Acme/Alpha"),
				label("Label_2", "Projects/Alpha"), label("Label_1", "Projects")));

		assertEquals("Label_2", GoogleGmailFolders.resolve(gmail, "alpha").labelId());
		assertEquals("Label_3", GoogleGmailFolders.resolve(gmail, "Clients/Acme/Alpha").labelId());
		assertEquals("Label_1", GoogleGmailFolders.resolve(gmail, "label_1").labelId());
	}

	@Test
	void aWholeNameWinsOverAnotherLabelsOwnName() {
		when(gmail.listLabels()).thenReturn(List.of(label("Label_2", "Projects/Alpha"), label("Label_9", "Alpha")));

		assertEquals("Label_9", GoogleGmailFolders.resolve(gmail, "Alpha").labelId());
	}

	@Test
	void wellKnownNamesNeedNoLabelsAndAnUnknownNameIsTakenAsAnId() {
		when(gmail.listLabels()).thenReturn(List.of(label("Label_1", "Projects")));

		assertEquals(GoogleGmailFolders.INBOX, GoogleGmailFolders.resolve(gmail, "inbox").labelId());
		GoogleGmailFolders.Target archive = GoogleGmailFolders.resolve(gmail, "archive");
		assertNull(archive.labelId());
		assertTrue(archive.archive());
		assertEquals("Label_77", GoogleGmailFolders.resolve(gmail, "Label_77").labelId());
	}

	private static Map<String, Object> label(String id, String name) {
		return Map.of("id", id, "name", name, "type", "user");
	}
}
