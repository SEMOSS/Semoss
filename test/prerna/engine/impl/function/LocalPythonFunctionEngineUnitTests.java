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
package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.auth.User;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;

public class LocalPythonFunctionEngineUnitTests extends SemossUnitTest {
	private Insight insight;
	private User user;
	private LocalPythonFunctionEngine engine;

	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new LocalPythonFunctionEngine();
		insight = mock(Insight.class);
	}

	@Test
	void testOpenWithProperties() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineAlias = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";
		String testPythonFileName = "test python file name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty("PYTHON_FILE_NAME", testPythonFileName);

		Path engineFolder = tempDir.resolve(Constants.FUNCTION_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path pyDir = engineFolder.resolve("py");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(IEngine.CATALOG_TYPE.class), anyString(),
						anyString())).thenReturn(engineFolder.toString());

				engine.setBasic(true);
				engine.open(testProps);
				Properties engineProps = engine.getSmssProp();
				for (Entry<Object, Object> testProp : testProps.entrySet()) {
					assertTrue(engineProps.containsKey(testProp.getKey()));
					assertTrue(engineProps.containsValue(testProp.getValue()));
				}
			}
		}
	}

	@Test
	void testOpenWithPropertiesNoPythonFunction() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineAlias = "engine_name";
		String functionName = "function_name";
		String functionDescription = "function_description";
		String testPythonFileName = "test python file name";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineAlias);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
//		testProps.setProperty("PYTHON_FILE_NAME", testPythonFileName);

		Path engineFolder = tempDir.resolve(Constants.FUNCTION_FOLDER)
				.resolve(SmssUtilities.getUniqueName(testEngineAlias, testEngine));
		Path pyDir = engineFolder.resolve("py");

		try (MockedStatic<DIHelper> dh = Mockito.mockStatic(DIHelper.class);) {
			DIHelper diMock = mock(DIHelper.class);
			dh.when(() -> DIHelper.getInstance()).thenReturn(diMock);
			when(diMock.getProperty(Constants.BASE_FOLDER)).thenReturn(engineFolder.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class);
					MockedStatic<HttpHelperUtility> hhu = Mockito.mockStatic(HttpHelperUtility.class);) {
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(IEngine.CATALOG_TYPE.class), anyString(),
						anyString())).thenReturn(engineFolder.toString());
				engine.setBasic(true);

				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> engine.open(testProps));
				assertEquals("Please enter the name of the python file used to instantiate the function.",
						e.getMessage());
			}
		}
	}
	
	@Test
	void testGetCatalogSubType() {
		assertEquals(FunctionTypeEnum.LOCAL_PYTHON.name(), engine.getCatalogSubType(null));
	}
}
