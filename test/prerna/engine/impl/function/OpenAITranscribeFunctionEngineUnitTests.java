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
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IFunctionEngine;
import prerna.om.Insight;

public class OpenAITranscribeFunctionEngineUnitTests {
	private Insight insight;
	private User user;
	private OpenAITranscribeFunctionEngine engine;
	
	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new OpenAITranscribeFunctionEngine();
		insight = mock(Insight.class);
	}
	
	@Test
	void testOpenWithProperties() throws Exception {
		Properties testProps = new Properties();
		String url = "url";
		String apiKey = "api_key";
		String functionName = "function_name";
		String descriptionKey = "description_key";

		testProps.setProperty("URL", url);
		testProps.setProperty("API_KEY", apiKey);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, descriptionKey);

		Gson gson = new Gson();
		String funcParamName = "func_param_name";
		String funcParamType = "func_param_type";
		String funcParamDesc = "func_param_desc";
		List<FunctionParameter> params = new Vector<>();
		FunctionParameter param = new FunctionParameter(funcParamName, funcParamType, funcParamDesc);
		params.add(param);
		testProps.setProperty(IFunctionEngine.PARAMETER_KEY, gson.toJson(params));

		List<String> requiredParams = new Vector<>();
		{
			for (int idx = 0; idx < 5; idx++) {
				requiredParams.add("required_param" + idx);
			}
		}
		testProps.setProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY, gson.toJson(requiredParams));

		engine.setBasic(true); // skips folder initialization
		engine.open(testProps);
		Properties engineProps = engine.getSmssProp();
		for (Entry<Object, Object> testProp : testProps.entrySet()) {
			assertTrue(engineProps.containsKey(testProp.getKey()));
			assertTrue(engineProps.containsValue(testProp.getValue()));
		}
		List<FunctionParameter> engineParams = engine.getParameters();
		// test engine parameters
		assertEquals(params.size(), engineParams.size());
		for (FunctionParameter engineParam : engineParams) {
			assertEquals(funcParamName, engineParam.getParameterName());
			assertEquals(funcParamType, engineParam.getParameterType());
			assertEquals(funcParamDesc, engineParam.getParameterDescription());
		}
		// test required parameters
		List<String> engineReqParams = engine.getRequiredParameters();
		assertEquals(requiredParams.size(), engineReqParams.size());
		for (int reqIdx = 0; reqIdx < engineReqParams.size(); reqIdx++) {
			assertEquals(requiredParams.get(reqIdx), engineReqParams.get(reqIdx));
		}
	}
	
	@Test
	void testOpenWithPropertiesNoAPIKey() throws Exception {
		Properties testProps = new Properties();
		String url = "url";
		String apiKey = "api_key";
		String functionName = "function_name";
		String descriptionKey = "description_key";

		testProps.setProperty("URL", url);
//		testProps.setProperty("API_KEY", apiKey);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, descriptionKey);

		Gson gson = new Gson();
		String funcParamName = "func_param_name";
		String funcParamType = "func_param_type";
		String funcParamDesc = "func_param_desc";
		List<FunctionParameter> params = new Vector<>();
		FunctionParameter param = new FunctionParameter(funcParamName, funcParamType, funcParamDesc);
		params.add(param);
		testProps.setProperty(IFunctionEngine.PARAMETER_KEY, gson.toJson(params));

		List<String> requiredParams = new Vector<>();
		{
			for (int idx = 0; idx < 5; idx++) {
				requiredParams.add("required_param" + idx);
			}
		}
		testProps.setProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY, gson.toJson(requiredParams));

		engine.setBasic(true); // skips folder initialization
		RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
		assertEquals("Must set API key. Use EMPTY if none.", e.getMessage());
	}
	
	@Test
	void testOpenWithPropertiesNoURL() throws Exception {
		Properties testProps = new Properties();
		String url = "url";
		String apiKey = "api_key";
		String functionName = "function_name";
		String descriptionKey = "description_key";

//		testProps.setProperty("URL", url);
		testProps.setProperty("API_KEY", apiKey);
		testProps.setProperty(IFunctionEngine.NAME_KEY, functionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, descriptionKey);

		Gson gson = new Gson();
		String funcParamName = "func_param_name";
		String funcParamType = "func_param_type";
		String funcParamDesc = "func_param_desc";
		List<FunctionParameter> params = new Vector<>();
		FunctionParameter param = new FunctionParameter(funcParamName, funcParamType, funcParamDesc);
		params.add(param);
		testProps.setProperty(IFunctionEngine.PARAMETER_KEY, gson.toJson(params));

		List<String> requiredParams = new Vector<>();
		{
			for (int idx = 0; idx < 5; idx++) {
				requiredParams.add("required_param" + idx);
			}
		}
		testProps.setProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY, gson.toJson(requiredParams));

		engine.setBasic(true); // skips folder initialization
		RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
		assertEquals("Must set URL", e.getMessage());
	}
	
	@Test
	void testGetCatalogSubType() {
		assertEquals(FunctionTypeEnum.OPENAI_TRANSCRIBE.name(), engine.getCatalogSubType(null));
	}
}
