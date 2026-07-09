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
package prerna.reactor.interceptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GenericGuardrailInputReactor extends AbstractReactor implements IInputReactor {

	public GenericGuardrailInputReactor() {
		// No keysToGet needed as we use ReactorInputHelper
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		ReactorInputHelper helper = new ReactorInputHelper(this.getNounStore());

		String guardrailEngineId = helper.getConfigParameter("guardrailEngineId", String.class);
		if (guardrailEngineId == null || guardrailEngineId.isEmpty()) {
			throw new SecurityException(
					"GenericGuardrailInputReactor is not configured correctly. Missing 'guardrailEngineId'.");
		}
		IGuardrailReactorFunctionEngine guardrailEngine = Utility.getGuardrailEngine(guardrailEngineId);
		if (guardrailEngine == null) {
			throw new SecurityException("Guardrail engine with ID '" + guardrailEngineId + "' not found.");
		}

		// Get the input mapping for the guardrail engine
		Map<String, Object> inputMapping = helper.getConfigParameter("inputMapping", Map.class);
		if (inputMapping == null) {
			inputMapping = new HashMap<>();
		}

		// TODO: how to incorporate masking ... or do we generate a new guardrail
		// instead...
		Boolean blockOnGuardrailFailure = helper.getConfigParameter("blockOnGuardrailFailure", Boolean.class);
		if (blockOnGuardrailFailure == null) {
			blockOnGuardrailFailure = true; // Default value
		}

		// A temporary map to hold the values for the current guardrail engine call
		Map<String, Object> guardrailEngineParams = new HashMap<>();

		// Process the inputMapping to get parameters from the intercepted method's
		// arguments
		for (Map.Entry<String, Object> entry : inputMapping.entrySet()) {
			String guardrailParamName = entry.getKey();
			Object mappedValue = entry.getValue();

			if (mappedValue instanceof String) {
				String argName = (String) mappedValue;
				Object argValue = helper.getMethodArgument(argName);
				guardrailEngineParams.put(guardrailParamName, argValue);
			} else if (mappedValue instanceof List) {
				List<?> argNames = (List<?>) mappedValue;
				StringBuilder combinedPrompt = new StringBuilder();
				boolean isStringCombination = true;

				for (Object name : argNames) {
					if (name instanceof String) {
						Object argValue = helper.getMethodArgument((String) name);
						if (argValue instanceof String) {
							combinedPrompt.append(argValue).append(" ");
						}
					} else {
						isStringCombination = false;
						break;
					}
				}

				if (isStringCombination && combinedPrompt.length() > 0) {
					guardrailEngineParams.put(guardrailParamName, combinedPrompt.toString().trim());
				} else {
					List<Object> values = new ArrayList<>();
					for (Object name : argNames) {
						if (name instanceof String) {
							values.add(helper.getMethodArgument((String) name));
						}
					}
					guardrailEngineParams.put(guardrailParamName, values);
				}
			} else {
				// This case should ideally not happen for inputMapping
				guardrailEngineParams.put(guardrailParamName, mappedValue);
			}
		}

		// Add direct parameters from the 'directParameters' map in the reactor's config
		Map<String, Object> directParameters = helper.getConfigParameter("directParameters", Map.class);
		if (directParameters != null) {
			guardrailEngineParams.putAll(directParameters);
		}

		// Now, prepare the NounStore for the guardrail engine
		NounStore guardrailInputNounStore = new NounStore("guardrailInput");
		for (Map.Entry<String, Object> paramEntry : guardrailEngineParams.entrySet()) {
			String paramName = paramEntry.getKey();
			Object paramValue = paramEntry.getValue();

			GenRowStruct nounGrs = guardrailInputNounStore.makeGenRowStruct(paramName);
			if (paramValue instanceof Collection) {
				Collection<Object> paramValueCollection = (Collection<Object>) paramValue;
				for (Object paramValueEle : paramValueCollection) {
					nounGrs.add(NounMetadata.predictNounMetadata(paramValueEle));
				}
			} else {
				nounGrs.add(NounMetadata.predictNounMetadata(paramValue));
			}
		}

		if (this.insight != null) {
			GenRowStruct insightGrs = guardrailInputNounStore.makeGenRowStruct(Constants.INSIGHT);
			insightGrs.add(NounMetadata.predictNounMetadata(this.insight));
		}

		// Call the guardrail engine's execute method
		GuardrailNounMetadata output = guardrailEngine.execute(guardrailInputNounStore, null);

		Map<String, Object> resultMap = createInterimResult(output, this.getClass().getName());

		// Update the processedArguments with the interim result
		Map<String, Object> processedArguments = helper.getArgumentsMap();
		processedArguments.put(PipelineReactorUtils.INTERIM_RESULT, resultMap);
		return new NounMetadata(processedArguments, PixelDataType.MAP);
	}

	/**
	 * Helper method to create the interim result map (already exists)
	 * 
	 * @param pass
	 * @param interceptorName
	 * @return
	 */
	private Map<String, Object> createInterimResult(GuardrailNounMetadata results, String interceptorName) {
		Map<String, Object> resultMap = new HashMap<>();
		resultMap.put(PipelineReactorUtils.INTERCEPTOR, interceptorName);
		resultMap.put(PipelineReactorUtils.PASS, results.isPass());
		resultMap.put(PipelineReactorUtils.PASS_DETAILS, results.getValue());

		return resultMap;
	}
}
