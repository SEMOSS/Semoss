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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class DefaultImageGeneratorUtilUnitTests {

	@TempDir
	Path temp;

	@Test
	void registeredSystemProjectsShareAThemeAwareBadgeOutsideTheStockCollection() throws Exception {
		Path directory = Files.createDirectories(temp.resolve("images/system-projects"));
		Path light = Files.writeString(directory.resolve("instance-managed-light.svg"), "light badge");
		Path dark = Files.writeString(directory.resolve("instance-managed-dark.svg"), "dark badge");
		createStockImages("stock-engines-light");
		createStockImages("stock-engines-dark");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			for (String id : new String[] { Constants.APP_REACT_TEMPLATE, Constants.SKILL_DATABASE,
					Constants.MCP_APP_FILESYSTEM, Constants.AGENT_APP_BUILDER }) {
				assertEquals(light.toFile(), DefaultImageGeneratorUtil.getSystemProjectImage(id, "light"));
				assertEquals(dark.toFile(), DefaultImageGeneratorUtil.getSystemProjectImage(id, " DARK "));
				File defaultImage = DefaultImageGeneratorUtil.getSystemProjectImage(id, null);
				assertEquals(defaultImage, DefaultImageGeneratorUtil.getSystemProjectImage(id, "system"));
				assertEquals(defaultImage, DefaultImageGeneratorUtil.getSystemProjectImage(id, "../../other"));
			}
			for (String id : new String[] { null, "", "user-project", "app-builder-copy", "platform__app-builder" }) {
				assertNull(DefaultImageGeneratorUtil.getSystemProjectImage(id, "light"));
			}
			assertEquals("stock-engines-light", DefaultImageGeneratorUtil
					.getStockImageForPath("user-project.png", "light").getParentFile().getName());
			assertFalse(Files.exists(temp.resolve("project")));
		}
	}

	@Test
	void systemBadgeFallsBackToAvailableThemeAndThenNormalProjectImages() throws Exception {
		Path directory = Files.createDirectories(temp.resolve("images/system-projects"));
		Path light = Files.writeString(directory.resolve("instance-managed-light.svg"), "light badge");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			assertEquals(light.toFile(),
					DefaultImageGeneratorUtil.getSystemProjectImage(Constants.SKILL_DATABASE, "dark"));
			Files.delete(light);
			Path dark = Files.writeString(directory.resolve("instance-managed-dark.svg"), "dark badge");
			assertEquals(dark.toFile(),
					DefaultImageGeneratorUtil.getSystemProjectImage(Constants.SKILL_DATABASE, "light"));
			Files.delete(dark);
			assertNull(DefaultImageGeneratorUtil.getSystemProjectImage(Constants.SKILL_DATABASE, "light"));
			assertFalse(Files.exists(temp.resolve("project")));
		}
	}

	@Test
	void repeatedLookupsReturnSharedFileWithoutCreatingProjectAssets() throws Exception {
		createStockImages("stock-engines-light");
		createStockImages("stock-engines-dark");
		Path version = temp.resolve("project/Example__id/app_root/version");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			File first = DefaultImageGeneratorUtil.getStockImageForPath(version.resolve("image.png").toString());
			File second = DefaultImageGeneratorUtil.getStockImageForPath(version.resolve("image.png").toString());
			assertNotNull(first);
			assertEquals(first, second);
			assertTrue(first.toPath().startsWith(temp.resolve("images")));
			assertTrue(first.isFile());
			assertFalse(Files.exists(temp.resolve("project")));
		}
	}

	@Test
	void referenceMatchesCopiedSelectionAndSurvivesProjectRename() throws Exception {
		createStockImages("stock-engines-light");
		createStockImages("stock-engines-dark");
		Path firstPath = temp.resolve("project/Example__first/app_root/version/image.png");
		Path secondPath = temp.resolve("project/Renamed__first/app_root/version/image.png");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			File reference = DefaultImageGeneratorUtil.getStockImageForPath(firstPath.toString());
			assertEquals(reference, DefaultImageGeneratorUtil.getStockImageForPath(secondPath.toString()));
			File legacyCopy = DefaultImageGeneratorUtil.pickRandomImage(firstPath.toString());
			assertArrayEquals(Files.readAllBytes(reference.toPath()), Files.readAllBytes(legacyCopy.toPath()));
			assertFalse(Files.exists(secondPath.getParent()));
		}
	}

	@Test
	void commonPrefixesAndSharedInternalAliasesReceiveDifferentArtwork() throws Exception {
		createStockImages("stock-engines-light");
		createStockImages("stock-engines-dark");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			for (String prefix : new String[] { "MCP ", "platform__project-" }) {
				Set<String> selected = new HashSet<>();
				for (String suffix : new String[] { "A", "B", "C" }) {
					String path = temp.resolve("project/" + prefix + suffix + "/app_root/version/image.png").toString();
					File light = DefaultImageGeneratorUtil.getStockImageForPath(path, "light");
					File dark = DefaultImageGeneratorUtil.getStockImageForPath(path, "dark");
					selected.add(light.getName());
					assertEquals(light.getName(), dark.getName());
					assertEquals(light, DefaultImageGeneratorUtil.getStockImageForPath(path, "light"));
				}
				assertEquals(3, selected.size(), "The A/B/C examples should not collapse for prefix " + prefix);
			}
			assertFalse(Files.exists(temp.resolve("project")));
		}
	}

	@Test
	void projectsSharingAnInternalAliasUseTheWholeStockCollection() throws Exception {
		createStockImages("stock-engines-light");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			Set<String> selected = new HashSet<>();
			for (int i = 0; i < 120; i++) {
				String path = temp.resolve("project/platform__project-" + i + "/app_root/version/image.png").toString();
				selected.add(DefaultImageGeneratorUtil.getStockImageForPath(path, "light").getName());
			}
			assertEquals(12, selected.size(), "Shared aliases must not force one stock image for every project");
		}
	}

	@Test
	void localClusterAndByteLookupsUseTheSameResourceIdentity() throws Exception {
		createStockImages("stock-engines-light");
		createStockImages("stock-engines-dark");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			for (String theme : new String[] { "light", "dark" }) {
				for (String alias : new String[] { "MCP A", "MCP B", "platform", "MCP__nested" }) {
					String path = temp.resolve("project/" + alias + "__resource-id/app_root/version/image.png")
							.toString();
					File local = DefaultImageGeneratorUtil.getStockImageForPath(path, theme);
					File cluster = DefaultImageGeneratorUtil
							.getStockImageForPath(temp.resolve("images/projects/resource-id.png").toString(), theme);
					assertEquals(local, cluster);
					assertArrayEquals(Files.readAllBytes(local.toPath()),
							DefaultImageGeneratorUtil.pickRandomImageBytes("resource-id", theme));
				}
			}
		}
	}

	@Test
	void requestThemeSelectsMatchingArtworkWithoutChangingTheDefault() throws Exception {
		createStockImages("stock-engines-light");
		createStockImages("stock-engines-dark");
		String path = temp.resolve("project/Example__id/app_root/version/image.png").toString();
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			File defaultImage = DefaultImageGeneratorUtil.getStockImageForPath(path);
			File light = DefaultImageGeneratorUtil.getStockImageForPath(path, "light");
			File dark = DefaultImageGeneratorUtil.getStockImageForPath(path, "dark");
			assertEquals(temp.resolve("images/stock-engines-light"), light.toPath().getParent());
			assertEquals(temp.resolve("images/stock-engines-dark"), dark.toPath().getParent());
			assertEquals(light.getName(), dark.getName());
			assertEquals(light, DefaultImageGeneratorUtil.getStockImageForPath(path, "light"));
			assertEquals(dark, DefaultImageGeneratorUtil.getStockImageForPath(path, " DARK "));
			assertEquals(defaultImage, DefaultImageGeneratorUtil.getStockImageForPath(path));
			for (String unsupported : new String[] { "", "system", "blue" }) {
				assertEquals(defaultImage, DefaultImageGeneratorUtil.getStockImageForPath(path, unsupported));
			}
			assertFalse(Files.exists(temp.resolve("project")));
		}
	}

	@Test
	void byteDownloadsUseTheRequestedTheme() throws Exception {
		createStockImages("stock-engines-light");
		createStockImages("stock-engines-dark");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			for (String theme : new String[] { "light", "dark" }) {
				File file = DefaultImageGeneratorUtil.getStockImageForPath("Example.png", theme);
				assertArrayEquals(Files.readAllBytes(file.toPath()),
						DefaultImageGeneratorUtil.pickRandomImageBytes("Example", theme));
			}
		}
	}

	@Test
	void missingRequestedCollectionFallsBackToConfiguredTheme() throws Exception {
		createStockImages("stock-engines-light");
		createStockImages("stock-engines-dark");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			File configured = DefaultImageGeneratorUtil.getStockImageForPath("Example.png");
			String missingTheme = configured.getParentFile().getName().endsWith("light") ? "dark" : "light";
			Path missingDirectory = temp.resolve("images/stock-engines-" + missingTheme);
			try (var files = Files.list(missingDirectory)) {
				for (Path file : files.toList()) {
					Files.delete(file);
				}
			}
			assertEquals(configured, DefaultImageGeneratorUtil.getStockImageForPath("Example.png", missingTheme));
		}
	}

	@Test
	void fallsBackToUnthemedCollection() throws Exception {
		createStockImages("stock-engines");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			File reference = DefaultImageGeneratorUtil.getStockImageForPath(temp.resolve("engine.png").toString());
			assertEquals(temp.resolve("images/stock-engines"), reference.toPath().getParent());
			assertEquals(reference,
					DefaultImageGeneratorUtil.getStockImageForPath(temp.resolve("engine.png").toString(), "dark"));
			assertFalse(Files.exists(temp.resolve("engine.png")));
		}
	}

	@Test
	void missingStockCollectionReturnsNullWithoutWriting() {
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			assertNull(DefaultImageGeneratorUtil.getStockImageForPath(temp.resolve("engine/image.png").toString()));
			assertFalse(Files.exists(temp.resolve("engine")));
			assertFalse(Files.exists(temp.resolve("images")));
		}
	}

	private void createStockImages(String directory) throws Exception {
		Path stock = Files.createDirectories(temp.resolve("images").resolve(directory));
		for (int i = 0; i < 12; i++) {
			Files.writeString(stock.resolve(i + ".png"), directory + "-image-" + i);
		}
	}
}
