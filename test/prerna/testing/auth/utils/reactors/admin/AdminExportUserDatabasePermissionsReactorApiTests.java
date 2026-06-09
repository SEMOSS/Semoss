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
package prerna.testing.auth.utils.reactors.admin;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestEngineUtilities;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.reactors.admin.AdminExportUserDatabasePermissionsReactor;
import prerna.om.Insight;

import prerna.sablecc2.om.NounStore;


public class AdminExportUserDatabasePermissionsReactorApiTests  extends AbstractBaseSemossApiTests {

	/*private FileSystem fs = Jimfs.newFileSystem(Configuration.unix());
/* private FileSystem fs = Jimfs.newFileSystem(Configuration.unix());

	private AdminExportUserDatabasePermissionsReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;
	private Path testFilePath;
	private String engine;

 
	@BeforeEach
	void setup() throws IOException {
		reactor = new AdminExportUserDatabasePermissionsReactor();
		reactor.setFileSystem(fs);
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);

		ns = mock(NounStore.class);
		reactor.setNounStore(ns); 

		Path p = fs.getPath("work", "insight1");
		Files.createDirectories(p);

	}

    @Test
    public void testAdminUtilsNullThrowsException() {
 
    	//testFilePath = Files.createTempFile("test-file", ".txt");
    	String engine = ApiSemossTestEngineUtils.createBasicEngine();
        when(SecurityAdminUtils.getInstance(user)).thenReturn(null); // Simulate adminUtils being null
        String pixel = ApiSemossTestUtils.buildPixelCall(AdminExportUserDatabasePermissionsReactor.class, ReactorKeysEnum.ENGINE.getKey(), engine);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    } */
    
    
   }
