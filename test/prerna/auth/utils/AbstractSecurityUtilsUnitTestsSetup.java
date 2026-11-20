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
package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import prerna.SemossUnitTest;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SystemEngineRegistry;
import prerna.util.SystemEngineRegistryTestExtension;

@ExtendWith(SystemEngineRegistryTestExtension.class)
public class AbstractSecurityUtilsUnitTestsSetup extends SemossUnitTest {

	static Path securityOwlFile = null;
	static String databaseFolder = null;
	static Path dbDir = null;

	private static final String fileSeparator = FileSystems.getDefault().getSeparator();

	@BeforeAll
	public static void createTempDbFolder() throws Exception {
		// set up base folders
		File baseFolder = new File(tempDir.toFile(), "semoss");
		baseFolder.mkdir();
		File dbFolder = new File(baseFolder, "db");
		dbFolder.mkdir();
		databaseFolder = dbFolder.getAbsolutePath();
		dbDir = dbFolder.toPath();

		// creating temp rdf file for DI Helper
		String rdfMap = tempDir + fileSeparator + "rdfMap.prop";
		File rdfMapFile = new File(tempDir + fileSeparator + "rdfMap.prop");
		Properties rdfMapProps = new Properties();
		rdfMapProps.setProperty(Constants.BASE_FOLDER, baseFolder.getAbsolutePath().toString());

		// save rdf map file
		try (FileOutputStream out = new FileOutputStream(rdfMapFile)) {
			rdfMapProps.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}
		DIHelper.getInstance().loadCoreProp(rdfMap);

		// adding security db to temp DI Helper
		File securityFolder = new File(dbFolder, "security");
		securityFolder.mkdir();

		// copy smss file to temp db folder
		Properties securityProps = UnitTestSecurityAuthUtils.getDefaultDBProperties("security");

		Path secSmss = UnitTestSecurityAuthUtils.createSmssFileFromProps(securityProps, dbFolder.getAbsolutePath(),
				"security.smss");

		SystemEngineRegistryTestExtension.loadForTesting(secSmss.toString());

		securityOwlFile = securityFolder.toPath().resolve("app_root").resolve("version").resolve("assets")
				.resolve("security_OWL.OWL");

		AbstractSecurityUtils.loadSecurityDatabase();
	}

	@AfterAll
	public static void tearDown() throws IOException, SQLException {
		IRDBMSEngine securityDb = (IRDBMSEngine) SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		try (Connection c = securityDb.getConnection(); Statement s = c.createStatement()) {
			assertTrue(c.getMetaData().getURL().contains("junit"));
			s.execute("SHUTDOWN");
		}
		DIHelper.getInstance();
		securityDb.closeDataSource();
		securityDb.close();
//		securityDb.delete();
	}

}
