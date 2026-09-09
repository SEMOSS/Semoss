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
package prerna.engine.impl.guardrail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.ds.py.PyUtils;
import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.engine.impl.function.FunctionParameter;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.util.Constants;

/**
 * Guardrail engine backed by a user supplied python file. Mirrors
 * {@link prerna.engine.impl.function.LocalPythonFunctionEngine} - the python
 * file is loaded once against the python process and the configured guardrail
 * function is invoked on each {@link #execute(NounStore, GenRowStruct)} call.
 * <p>
 * The python guardrail function is required to return a dict whose shape maps
 * directly onto {@link GuardrailNounMetadata}:
 *
 * <pre>
 * def my_guardrail(prompt, **kwargs):
 *     ...
 *     return {
 *         "pass": True,                 # required boolean - did the prompt pass the guardrail
 *         "returnPrompt": prompt,       # optional string - the (possibly modified) prompt to return
 *         "fullDetails": { ... }        # optional - any details to surface back to the caller
 *     }
 * </pre>
 */
public class LocalPythonGuardrailReactorFunctionEngine extends AbstractPythonGuardrailReactorFunctionEngine
		implements IGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(LocalPythonGuardrailReactorFunctionEngine.class);

	protected static final String INIT_FUNCTION_ENGINE = "INIT_FUNCTION_ENGINE";
	protected static final String PYTHON_FILE_NAME = "PYTHON_FILE_NAME";

	/**
	 * Default prompt parameter name. When the python function does not return an
	 * explicit {@code returnPrompt} we fall back to whatever was passed under this
	 * key so callers still receive the prompt they submitted.
	 */
	protected static final String PROMPT_KEY = "prompt";

	protected String pythonFileName;

	// string substitute vars for the init commands
	protected Map<String, String> vars = new HashMap<>();

	@Override
	public void open(Properties smssProp) throws Exception {
		// sets engineDirectoryPath + cacheFolder
		super.open(smssProp);

		this.pythonFileName = smssProp.getProperty(PYTHON_FILE_NAME, null);
		if (this.pythonFileName == null) {
			throw new IllegalArgumentException(
					"Please enter the name of the python file used to instantiate the guardrail.");
		}

		// the guardrail engine does not run through the function engine open()
		// so pull the function definition off the smss ourselves
		this.functionName = smssProp.getProperty(IFunctionEngine.NAME_KEY);
		if (this.functionName == null || (this.functionName = this.functionName.trim()).isEmpty()) {
			throw new IllegalArgumentException(
					"Must define the guardrail function name via " + IFunctionEngine.NAME_KEY);
		}
		this.functionDescription = smssProp.getProperty(IFunctionEngine.DESCRIPTION_KEY);

		if (smssProp.containsKey(IFunctionEngine.PARAMETER_KEY)) {
			this.parameters = new Gson().fromJson(smssProp.getProperty(IFunctionEngine.PARAMETER_KEY),
					new TypeToken<List<FunctionParameter>>() {
					}.getType());
		}
		if (smssProp.containsKey(IFunctionEngine.REQUIRED_PARAMETER_KEY)) {
			this.requiredParameters = new Gson().fromJson(smssProp.getProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY),
					new TypeToken<List<String>>() {
					}.getType());
		}

		// derive the keys (and which are required) from the declared parameters so
		// organizeKeys / execute can map nouns onto the python function arguments
		if (this.parameters != null && !this.parameters.isEmpty()) {
			this.keysToGet = new String[this.parameters.size()];
			this.keyRequired = new int[this.parameters.size()];
			for (int i = 0; i < this.parameters.size(); i++) {
				String paramName = this.parameters.get(i).getParameterName();
				this.keysToGet[i] = paramName;
				this.keyRequired[i] = (this.requiredParameters != null && this.requiredParameters.contains(paramName))
						? 1
						: 0;
			}
		}

		// vars for string substitution within the init commands
		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}
	}

	@Override
	protected String getStartupScript() {
		// @formatter:off
		String execCommand = "import sys\n"
				+ "import os\n"
				+ "sys.path.append('" + this.engineDirectoryPath + "')\n"
				+ "sys.path.append('" + this.engineDirectoryPath + "/py')\n"
				+ "os.chdir('" + this.engineDirectoryPath + "')\n"
				+ "exec(open('" + this.engineDirectoryPath + "/" + this.pythonFileName + "').read())";
		// @formatter:on

		// append any additional initialization commands
		String initCommands = this.smssProp.getProperty(INIT_FUNCTION_ENGINE);
		if (initCommands != null && !(initCommands = initCommands.trim()).isEmpty()) {
			// break the commands separated by the python command separator
			String[] commands = initCommands.split(PyUtils.PY_COMMAND_SEPARATOR);
			for (int commandIndex = 0; commandIndex < commands.length; commandIndex++) {
				execCommand += "\n" + fillSmssVars(commands[commandIndex]);
			}
		}

		return execCommand;
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
		checkSocketStatus();

		// map the incoming nouns onto the python function's keyword arguments
		Map<String, Object> kwargs = organizeKeyObjects(ns, curRow);

		StringBuilder callMaker = new StringBuilder(this.functionName);
		callMaker.append("(**").append(PyUtils.determineStringType(kwargs)).append(")");

		Object resultObj = this.pyTranslator.runDirectPyNoCancelTrace(getExecutionInsight(ns), callMaker.toString());

		return hydrateGuardrailResult(resultObj, kwargs);
	}

	private Insight getExecutionInsight(NounStore ns) {
		if (ns == null) {
			return null;
		}
		GenRowStruct insightGrs = ns.getGenRowStruct(Constants.INSIGHT);
		if (insightGrs == null || insightGrs.isEmpty()) {
			return null;
		}
		Object insightObj = insightGrs.get(0);
		if (insightObj instanceof Insight) {
			return (Insight) insightObj;
		}
		return null;
	}

	/**
	 * Hydrate the dict returned from the python guardrail function into a
	 * {@link GuardrailNounMetadata}.
	 *
	 * @param resultObj the raw object returned from python - must be a dict/map
	 * @param kwargs    the arguments that were passed to the python function, used
	 *                  to default the return prompt
	 * @return the hydrated guardrail metadata
	 */
	@SuppressWarnings("unchecked")
	protected GuardrailNounMetadata hydrateGuardrailResult(Object resultObj, Map<String, Object> kwargs) {
		if (!(resultObj instanceof Map)) {
			throw new IllegalArgumentException(
					"The guardrail function '" + this.functionName + "' must return a dict but returned: " + resultObj);
		}

		Map<String, Object> result = (Map<String, Object>) resultObj;

		if (!result.containsKey(GuardrailNounMetadata.PASS_KEY)) {
			throw new IllegalArgumentException("The guardrail function '" + this.functionName
					+ "' must return a dict containing the key '" + GuardrailNounMetadata.PASS_KEY + "'");
		}
		boolean pass = parsePass(result.get(GuardrailNounMetadata.PASS_KEY));

		// the return prompt is optional - if the function did not manipulate/return
		// the prompt then fall back to whatever prompt was submitted
		String returnPrompt;
		Object returnPromptObj = result.get(GuardrailNounMetadata.RETURN_PROMPT_KEY);
		if (returnPromptObj != null) {
			returnPrompt = returnPromptObj.toString();
		} else {
			Object submittedPrompt = kwargs.get(PROMPT_KEY);
			returnPrompt = submittedPrompt != null ? submittedPrompt.toString() : null;
		}

		Object fullDetails = result.get(GuardrailNounMetadata.FULL_DETAILS_KEY);

		return new GuardrailNounMetadata(pass, returnPrompt, fullDetails);
	}

	/**
	 * Build the keyword argument map to send to the python function. Mirrors
	 * {@link #organizeKeys(NounStore, GenRowStruct)} but preserves the underlying
	 * object types (rather than coercing everything to a String) so the python
	 * function receives proper numbers, lists, etc.
	 */
	protected Map<String, Object> organizeKeyObjects(NounStore ns, GenRowStruct curRow) {
		Map<String, Object> kwargs = new HashMap<>();
		if (this.keysToGet == null || this.keysToGet.length == 0) {
			return kwargs;
		}

		// named nouns
		if (ns.size() > 0) {
			for (String key : this.keysToGet) {
				GenRowStruct grs = ns.getGenRowStruct(key);
				if (grs != null && !grs.isEmpty()) {
					kwargs.put(key, extractNounValue(grs));
				}
			}
		}

		// fill in order based on whatever is left
		int counter = 0;
		if (curRow != null && !curRow.isEmpty()) {
			for (String key : this.keysToGet) {
				if (!kwargs.containsKey(key)) {
					kwargs.put(key, curRow.get(counter));
					counter++;
				}
				if (counter >= curRow.size()) {
					break;
				}
			}
		}

		return kwargs;
	}

	/**
	 * Pull the value(s) off a GenRowStruct - a single entry is returned as a scalar
	 * while multiple entries are returned as a list.
	 */
	private Object extractNounValue(GenRowStruct grs) {
		if (grs.size() == 1) {
			return grs.get(0);
		}
		List<Object> values = new ArrayList<>();
		for (int i = 0; i < grs.size(); i++) {
			values.add(grs.get(i));
		}
		return values;
	}

	private boolean parsePass(Object value) {
		if (value instanceof Boolean) {
			return ((Boolean) value).booleanValue();
		}
		if (value == null) {
			throw new IllegalArgumentException("The guardrail function '" + this.functionName + "' returned a null '"
					+ GuardrailNounMetadata.PASS_KEY + "' value");
		}
		return Boolean.parseBoolean(value.toString().trim());
	}

	protected String fillSmssVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(this.vars);
		return sub.replace(input);
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.LOCAL_PYTHON;
	}

	@Override
	public String getDefaultMarkdown() {
		return """
				# Local Python guardrail

				This guardrail calls `%s` from `%s` with the parameters declared in the engine SMSS. The function must return a dictionary with a boolean `pass`. It may also return `returnPrompt` for masking or a user-facing response, and `fullDetails` for audit context.

				## Python function shape

				```python
				import re

				API_KEY = re.compile(r"(?i)(api_key\\s*=\\s*)[^\\s,;]+")

				def guard_prompt(prompt, **kwargs):
				    contains_secret = API_KEY.search(prompt) is not None
				    masked = API_KEY.sub(r"\\1[masked]", prompt)
				    return {
				        "pass": not contains_secret,
				        "returnPrompt": masked,
				        "fullDetails": {"rule": "api-key-prefix"},
				    }
				```

				Declare `prompt` in `FUNCTION_PARAMETERS` and `FUNCTION_REQUIRED_PARAMETERS`, and set `FUNCTION_NAME` to the actual Python function name.

				## Example: mask model input with the Python result

				Save this as `pipeline.json` in the model engine's assets folder, set `PIPELINE pipeline.json` in that model engine's SMSS, and restart or reload the model engine:

				```json
				{
				  "pipelines": {
				    "askRoom": {
				      "input": [
				        {
				          "reactorClass": "prerna.reactor.interceptor.GenericGuardrailInputReactor",
				          "params": {
				            "guardrailEngineId": "%s",
				            "inputMapping": {
				              "prompt": "arg0"
				            },
				            "maskOnGuardrailFailure": true,
				            "blockOnGuardrailFailure": false
				          }
				        }
				      ]
				    }
				  }
				}
				```

				For `askRoom`, `arg0` is the full `InputMessage`. The interceptor gives its text to the Python `prompt` parameter and, when masking, writes `returnPrompt` back into that same message before refreshing the provider payload. For other methods, map each declared Python parameter to the appropriate argument or nested map path.
				"""
				.formatted(this.functionName, this.pythonFileName, getEngineId());
	}

}
