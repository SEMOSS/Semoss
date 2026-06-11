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
package prerna.testing;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ApiRTestUtils {

	private static final Logger classLogger = LogManager.getLogger(ApiRTestUtils.class);

	public static void setup() throws Exception {
		classLogger.info("Checking and setting up R");
		Path folderInSemoss = Paths.get(ApiTestsSemossConstants.BASE_DIRECTORY, "R");
		if (Files.exists(folderInSemoss)) {
			classLogger.info("R folder in base directory exists, setting up R.");
			copyDirectory();
		} else {
			classLogger.warn("R folder in base directory does not exist, R will NOT be setup.");
		}

	}
	
	private static void copyDirectory() throws Exception {
		Path source = Paths.get(ApiTestsSemossConstants.BASE_DIRECTORY, "R");
		Path test = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "R");

		classLogger.info("SOURCE: {} \n TEST: {}", source, test);
		if (Files.exists(test)) {
			classLogger.info("Test folder exists, cleaning");
			FileUtils.cleanDirectory(test.toFile());
		}

		classLogger.info("Copying source to test workspace");
		FileUtils.copyDirectory(source.toFile(), test.toFile());

	}


}
