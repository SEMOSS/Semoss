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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.om.Insight;

class AutomationFileManifestServiceUnitTests {

	@TempDir
	Path root;

	@Test
	void capturesGovernedLocatorBytesAndChecksumWithoutRawPath() throws Exception {
		Files.createDirectories(root.resolve("downloads"));
		Files.writeString(root.resolve("downloads/result.txt"), "saved");
		Insight insight = mock(Insight.class);
		when(insight.getInsightFolder()).thenReturn(root.toString());

		Map<String, Object> captured = AutomationFileManifestService.capture(insight,
				Map.of("files", List.of("/downloads/result.txt"), "filePath", "/downloads/result.txt"), "node");

		Map<?, ?> file = (Map<?, ?>) captured.get("file");
		assertEquals("INSIGHT", file.get("space"));
		assertEquals("/downloads/result.txt", file.get("path"));
		assertEquals(5L, file.get("byteCount"));
		assertEquals(64, ((String) file.get("checksum")).length());
		assertFalse(captured.containsKey("filePath"));
		assertFalse(captured.toString().contains(root.toString()));
	}

	@Test
	void rejectsTraversalOutsideInsightAssets() throws Exception {
		Insight insight = mock(Insight.class);
		when(insight.getInsightFolder()).thenReturn(root.toString());

		assertThrows(IllegalStateException.class, () -> AutomationFileManifestService.capture(insight,
				Map.of("files", List.of("../secret.txt")), "node"));
	}
}
