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
package prerna.reactor.engine;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.AbstractPythonModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class EnginePyReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EnginePyReactor.class);

	public EnginePyReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CODE.getKey(), ReactorKeysEnum.ENGINE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}

		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have editor access to this model");
		}

		String code = this.keyValue.get(ReactorKeysEnum.CODE.getKey());
		if (code == null || code.trim().isEmpty()) {
			throw new IllegalArgumentException("Code parameter cannot be null or empty");
		}

		IEngine rawEngine = Utility.getEngine(engineId);
		if (!(rawEngine instanceof AbstractPythonModelEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a Python model engine");
		}
		AbstractPythonModelEngine engine = (AbstractPythonModelEngine) rawEngine;

		PyTranslator enginePyTranslator = null;
		Object output = null;
		try {
			enginePyTranslator = engine.getEnginePyTranslator();
			output = enginePyTranslator.runScript(code);
		} catch (IllegalArgumentException e) {
			classLogger.warn("Invalid argument when getting PyTranslator for engine {}: {}", engineId, e.getMessage());
			throw e;
		} catch (IllegalStateException e) {
			classLogger.error("Engine {} is not properly initialized or connection failed: {}", engineId,
					e.getMessage());
			throw new IllegalArgumentException(
					"Engine " + engineId + " is currently unavailable. Please try again later.", e);

		} catch (Exception e) {
			classLogger.error("Unexpected error executing code on engine {}: {}", engineId, e.getMessage(), e);
			throw new IllegalArgumentException("Failed to execute code on engine " + engineId + ": " + e.getMessage(),
					e);
		}

		if (output == null) {
			classLogger.warn("Code execution returned null output for engine {}", engineId);
			output = "";
		}

		List<NounMetadata> outputs = new ArrayList<>(1);
		outputs.add(new NounMetadata(output + "", PixelDataType.CONST_STRING));
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.CODE.getKey())) {
			return """
					The python code to execute against the engine. \
					For convenience, instead of escaping quotes or backslashes you can wrap \
					the input within "<encode>your_text</encode>" and the system will encode it for you.
					""";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(ReactorKeysEnum.CODE.getKey()).put("description",
				"The python code to execute against the engine.");
		return properties;
	}
}
