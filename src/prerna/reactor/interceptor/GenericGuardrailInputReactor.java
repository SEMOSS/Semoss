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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

	private static final Logger classLogger = LogManager.getLogger(GenericGuardrailInputReactor.class);

	// default guardrail input param whose mapped argument gets overwritten when masking
	private static final String DEFAULT_MASK_TARGET_PARAM = "prompt";

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

		Map<String, Object> processedArguments = helper.getArgumentsMap();

		// If this filter is configured to mask (rather than block) on failure, overwrite
		// the guarded argument with the masked prompt the engine produced so the masked
		// text - not the original - flows downstream to the model.
		boolean masked = false;
		Boolean maskOnGuardrailFailure = helper.getConfigParameter("maskOnGuardrailFailure", Boolean.class);
		if (maskOnGuardrailFailure != null && maskOnGuardrailFailure && !output.isPass()) {
			String maskTargetParam = helper.getConfigParameter("maskTargetParam", String.class);
			if (maskTargetParam == null || maskTargetParam.isEmpty()) {
				maskTargetParam = DEFAULT_MASK_TARGET_PARAM;
			}
			// inputMapping maps the guardrail param (e.g. "prompt") to the intercepted
			// method argument name (e.g. "arg0"); that argument is what we overwrite
			Object mappedArg = inputMapping.get(maskTargetParam);
			String maskedPrompt = output.getReturnPrompt();
			if (mappedArg instanceof String && maskedPrompt != null) {
				processedArguments.put((String) mappedArg, maskedPrompt);
				masked = true;
			} else {
				// cannot safely write the masked value back (e.g. a combined multi-arg
				// mapping) - leave pass as-is so the request is blocked rather than
				// leaking unmasked content downstream
				classLogger.warn(
						"maskOnGuardrailFailure is enabled but mask target '{}' is not mapped to a single argument; "
								+ "blocking instead of masking.",
						maskTargetParam);
			}
		}

		// When configured to respond (rather than mask or block), hand the guardrail's
		// message back as the model's answer. The real model call is skipped entirely,
		// so no version of the prompt reaches the provider.
		String cannedResponse = null;
		Boolean respondWithGuardrailMessage = helper.getConfigParameter("respondWithGuardrailMessage", Boolean.class);
		if (Boolean.TRUE.equals(respondWithGuardrailMessage) && !output.isPass()) {
			String candidate = output.getReturnPrompt();
			Object original = guardrailEngineParams.get(DEFAULT_MASK_TARGET_PARAM);
			if (candidate != null && !candidate.equals(original)) {
				cannedResponse = candidate;
			} else {
				classLogger.warn("respondWithGuardrailMessage is enabled but guardrail engine '{}' returned the input "
						+ "unchanged; blocking instead of responding.", guardrailEngineId);
			}
		}

		Boolean closeRoomOnBlock = helper.getConfigParameter("closeRoomOnBlock", Boolean.class);
		String blockErrorMessage = helper.getConfigParameter("blockErrorMessage", String.class);

		Map<String, Object> resultMap = createInterimResult(output, this.getClass().getName(), masked,
				cannedResponse, closeRoomOnBlock, blockErrorMessage);

		// Update the processedArguments with the interim result
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
	private Map<String, Object> createInterimResult(GuardrailNounMetadata results, String interceptorName,
			boolean masked, String cannedResponse, Boolean closeRoomOnBlock, String blockErrorMessage) {
		Map<String, Object> resultMap = new HashMap<>();
		resultMap.put(PipelineReactorUtils.INTERCEPTOR, interceptorName);
		// when we masked the input we neutralized the failure, so let it pass downstream
		resultMap.put(PipelineReactorUtils.PASS, masked || results.isPass());
		resultMap.put(PipelineReactorUtils.PASS_DETAILS, results.getValue());
		resultMap.put(PipelineReactorUtils.MASKED, masked);
		if (cannedResponse != null) {
			resultMap.put(PipelineReactorUtils.SHORT_CIRCUIT_RESPONSE, cannedResponse);
		}
		if (Boolean.TRUE.equals(closeRoomOnBlock) && !results.isPass() && !masked && cannedResponse == null) {
			resultMap.put(PipelineReactorUtils.CLOSE_ROOM, true);
		}
		if (blockErrorMessage != null && !blockErrorMessage.isEmpty()) {
			resultMap.put(PipelineReactorUtils.BLOCK_ERROR_MESSAGE, blockErrorMessage);
		}

		return resultMap;
	}
}
