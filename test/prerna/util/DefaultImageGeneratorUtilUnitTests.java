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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class DefaultImageGeneratorUtilUnitTests {

	@TempDir
	Path temp;

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
	void referenceMatchesLegacySelectionAndDoesNotDependOnProjectId() throws Exception {
		createStockImages("stock-engines-light");
		createStockImages("stock-engines-dark");
		Path firstPath = temp.resolve("project/Example__first/app_root/version/image.png");
		Path secondPath = temp.resolve("project/Example__second/app_root/version/image.png");
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
	void fallsBackToUnthemedCollection() throws Exception {
		createStockImages("stock-engines");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
			File reference = DefaultImageGeneratorUtil.getStockImageForPath(temp.resolve("engine.png").toString());
			assertEquals(temp.resolve("images/stock-engines"), reference.toPath().getParent());
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
			Files.writeString(stock.resolve(i + ".png"), "stock-image-" + i);
		}
	}
}
