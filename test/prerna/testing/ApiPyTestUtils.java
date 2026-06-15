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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.om.ClientProcessWrapper;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;

public class ApiPyTestUtils {

	private static final Logger classLogger = LogManager.getLogger(ApiPyTestUtils.class);

	public static void setupAndCheck() throws Exception {
		classLogger.info("Checking and setting up Python");
		Path pyFolderInSemoss = Paths.get(ApiTestsSemossConstants.BASE_DIRECTORY, "py");
		if (Files.exists(pyFolderInSemoss)) {
			classLogger.info("Py folder in base directory exists, setting up Python.");
			copyPyDirectory();

			classLogger.info("Python pixel check");
			basicPythonPixelCheck();

			classLogger.info("Teardown py server");
			teardownPyServer();
		} else {
			classLogger.warn("Py folder in base directory does not exist, Python will NOT be setup.");
		}

	}

	private static void teardownPyServer() {
		try {

			classLogger.info("Killing python process for user");
			User user = ApiSemossTestUserUtils.getUser();

			ClientProcessWrapper cpw = user.getPythonClientProcessWrapper();

			if (cpw != null) {

				SocketClient sc = cpw.getSocketClient();
				if (sc != null && sc.isConnected() && sc.isKillAll()) {
					classLogger.info("Closing socket client");
					cpw.getSocketClient().close();
				}

				classLogger.info("Closing CPW");
				if (cpw.getProcess() != null) {
					classLogger.info("Destroying cpw process");
					cpw.getProcess().destroyForcibly();
				}

			}

		} catch (Exception e) {
			classLogger.fatal("COULD NOT KILL PYTHON PROCESS <<<<< LOOK AT THIS PLEASE", e);
		}
	}

	private static void basicPythonPixelCheck() {
		String pixel = "Py(\"<encode>1+1</encode>\");";
		classLogger.info("Running basic python pixel");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		List<NounMetadata> val = (List<NounMetadata>) nm.getValue();
		assertEquals(2, Integer.valueOf(val.get(0).getValue().toString()).intValue());
		classLogger.info("basic python pixel check passed");
	}

	private static void copyPyDirectory() throws Exception {
		Path sourcePy = Paths.get(ApiTestsSemossConstants.BASE_DIRECTORY, "py");
		Path testPy = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "py");

		classLogger.info("SOURCE: {} \n TEST: {}", sourcePy, testPy);
		if (Files.exists(testPy)) {
			classLogger.info("Test folder exists, cleaning");
			FileUtils.cleanDirectory(testPy.toFile());
		}

		classLogger.info("Copying source to test workspace (excluding install_config)");
		FileFilter excludeInstallConfig = pathname -> !"install_config".equals(pathname.getName());

		long copyStart = System.currentTimeMillis();
		FileUtils.copyDirectory(sourcePy.toFile(), testPy.toFile(), excludeInstallConfig);
		long copyDurationMs = System.currentTimeMillis() - copyStart;
		classLogger.info("Copy completed in {} ms ({} s)", copyDurationMs, copyDurationMs / 1000.0);

	}

}
