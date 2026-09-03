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
package prerna.engine.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ModelModalityEnumUnitTests {

	@Test
	void parsesMetadataAndCatalogNames() {
		assertEquals(ModelModalityEnum.PDF, ModelModalityEnum.fromName(" pdf "));
		assertEquals(ModelModalityEnum.IMAGE, ModelModalityEnum.fromName("IMAGE"));
		assertEquals("image", ModelModalityEnum.IMAGE.getCatalogName());
	}

	@Test
	void rejectsUnknownNames() {
		assertThrows(IllegalArgumentException.class, () -> ModelModalityEnum.fromName("spreadsheet"));
	}

	@Test
	void classifiesMimeTypes() {
		assertEquals(ModelModalityEnum.IMAGE, ModelModalityEnum.fromMimeType("image/png"));
		assertEquals(ModelModalityEnum.AUDIO, ModelModalityEnum.fromMimeType("audio/mpeg"));
		assertEquals(ModelModalityEnum.VIDEO, ModelModalityEnum.fromMimeType("video/mp4"));
		assertEquals(ModelModalityEnum.PDF, ModelModalityEnum.fromMimeType("application/pdf"));
		assertEquals(ModelModalityEnum.TEXT, ModelModalityEnum.fromMimeType("text/plain"));
		assertEquals(ModelModalityEnum.FILE, ModelModalityEnum.fromMimeType("application/msword"));
	}

	@Test
	void toleratesMimeParametersCasingAndWhitespace() {
		assertEquals(ModelModalityEnum.PDF, ModelModalityEnum.fromMimeType("application/pdf; name=report.pdf"));
		assertEquals(ModelModalityEnum.IMAGE, ModelModalityEnum.fromMimeType(" IMAGE/JPEG "));
	}

	@Test
	void unknownMimeTypesClassifyAsNull() {
		assertNull(ModelModalityEnum.fromMimeType(null));
		assertNull(ModelModalityEnum.fromMimeType("  "));
		assertNull(ModelModalityEnum.fromMimeType("application/octet-stream"));
		assertNull(ModelModalityEnum.fromMimeType("not a mime type"));
	}
}
