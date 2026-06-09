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
package prerna.testing.reactor.algorithms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import prerna.reactor.algorithms.LoadNLPSearchReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUserUtils;
import prerna.testing.ApiSemossTestUtils;

@Disabled
public class LoadNLPSearchReactorApiTests extends AbstractBaseSemossApiTests {
	
	@Test
	public void execute() {
		ApiSemossTestEngineUtils.createBasicEngine();
		String pixel = ApiSemossTestUtils.buildPixelCall(LoadNLPSearchReactor.class);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertTrue(Boolean.valueOf(nm.getValue().toString()));
		assertEquals(PixelDataType.BOOLEAN, nm.getNounType());
		
		Path p = ApiSemossTestUserUtils.getAssetsPath();
		assertTrue(Files.exists(p));
		assertTrue(Files.isDirectory(p));
		
		assertTrue(Files.exists(Paths.get(p.toString(), "nldr_membership.rds")));
		assertTrue(Files.isRegularFile(Paths.get(p.toString(), "nldr_membership.rds")));

		assertTrue(Files.exists(Paths.get(p.toString(), "nldr_db.rds")));
		assertTrue(Files.isRegularFile(Paths.get(p.toString(), "nldr_db.rds")));

		assertTrue(Files.isRegularFile(Paths.get(p.toString(), "nldr_joins.rds")));
		assertTrue(Files.isRegularFile(Paths.get(p.toString(), "nldr_joins.rds")));
	}
	
	@Test
	public void executeNoEngines() {
		String pixel = ApiSemossTestUtils.buildPixelCall(LoadNLPSearchReactor.class);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertTrue(Boolean.valueOf(nm.getValue().toString()));
		assertEquals(PixelDataType.BOOLEAN, nm.getNounType());
		
		Path p = ApiSemossTestUserUtils.getAssetsPath();
		assertTrue(Files.exists(p));
		assertTrue(Files.isDirectory(p));
		
		assertTrue(Files.exists(Paths.get(p.toString(), "nldr_membership.rds")));
		assertTrue(Files.isRegularFile(Paths.get(p.toString(), "nldr_membership.rds")));

		assertTrue(Files.exists(Paths.get(p.toString(), "nldr_db.rds")));
		assertTrue(Files.isRegularFile(Paths.get(p.toString(), "nldr_db.rds")));

		assertTrue(Files.isRegularFile(Paths.get(p.toString(), "nldr_joins.rds")));
		assertTrue(Files.isRegularFile(Paths.get(p.toString(), "nldr_joins.rds")));
	}


}
