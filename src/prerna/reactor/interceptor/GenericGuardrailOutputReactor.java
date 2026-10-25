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

import prerna.engine.api.IEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Screens the return value of an intercepted engine method. A failing guardrail
 * blocks that return value, which is the only outcome available once the method
 * has run - there is no pending call left to mask or to answer on the caller's
 * behalf.
 *
 * Any engine type can attach this to a pipeline slot. The intercepted method's
 * return value is exposed as {@code result}, alongside the method's arguments.
 *
 * A return value that wraps its payload is unwrapped before the guardrail sees
 * it, so a mapping to {@code result} resolves to the payload rather than to the
 * wrapper; without that step the guardrail receives the object's default string
 * form and passes everything. A dot path continues into the payload once it is
 * unwrapped.
 */
public class GenericGuardrailOutputReactor extends AbstractReactor implements IOutputReactor {

	public static final String RETURN_PROMPT_KEY = "returnPrompt";
	public static final String FULL_DETAILS_KEY = "fullDetails";
	private transient IEngine targetEngine;

	public GenericGuardrailOutputReactor() {
		// No keysToGet needed as we use ReactorInputHelper
		this.keysToGet = new String[] {};
	}

	public void setTargetEngine(IEngine targetEngine) {
		this.targetEngine = targetEngine;
	}

	@Override
	public NounMetadata execute() {
		ReactorInputHelper helper = new ReactorInputHelper(this.getNounStore());
		String guardrailEngineId = helper.getConfigParameter("guardrailEngineId", String.class);
		if (guardrailEngineId == null || guardrailEngineId.isEmpty()) {
			throw new SecurityException(
					"GenericGuardrailOutputReactor is not configured correctly. Missing 'guardrailEngineId'.");
		}
		IGuardrailReactorFunctionEngine guardrailEngine = Utility.getGuardrailEngine(guardrailEngineId);
		if (guardrailEngine == null) {
			throw new SecurityException("Guardrail engine with ID '" + guardrailEngineId + "' not found.");
		}

		Map<String, Object> inputMapping = helper.getConfigParameter("inputMapping", Map.class);
		if (inputMapping == null) {
			inputMapping = new HashMap<>();
		}

		// The values for this guardrail engine call, keyed by the parameter name the
		// guardrail reads.
		Map<String, Object> guardrailEngineParams = new HashMap<>();
		for (Map.Entry<String, Object> entry : inputMapping.entrySet()) {
			String guardrailParamName = entry.getKey();
			Object mappedValue = entry.getValue();

			if (mappedValue instanceof String) {
				guardrailEngineParams.put(guardrailParamName, resolveArgument(helper, (String) mappedValue));
			} else if (mappedValue instanceof List) {
				List<?> argNames = (List<?>) mappedValue;
				StringBuilder combinedPrompt = new StringBuilder();
				boolean isStringCombination = true;

				for (Object name : argNames) {
					if (name instanceof String) {
						Object argValue = resolveArgument(helper, (String) name);
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
							values.add(resolveArgument(helper, (String) name));
						}
					}
					guardrailEngineParams.put(guardrailParamName, values);
				}
			} else {
				// a mapping value is a name or a list of names, so anything else is
				// passed through as the literal it appears to be
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
				Collection<?> paramValueCollection = (Collection<?>) paramValue;
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

		// The target engine is direct invocation context, never serialized input.
		GuardrailNounMetadata output = guardrailEngine.execute(guardrailInputNounStore, null, this.targetEngine);

		Boolean closeRoomOnBlock = helper.getConfigParameter("closeRoomOnBlock", Boolean.class);
		Boolean logoutOnBlock = helper.getConfigParameter("logoutOnBlock", Boolean.class);
		String blockErrorMessage = helper.getConfigParameter("blockErrorMessage", String.class);

		// When configured to respond (rather than block), hand the guardrail's own
		// message back as the result in place of the real model response.
		String cannedResponse = null;
		Boolean respondWithGuardrailMessage = helper.getConfigParameter("respondWithGuardrailMessage", Boolean.class);
		if (Boolean.TRUE.equals(respondWithGuardrailMessage) && !output.isPass()) {
			cannedResponse = output.getReturnPrompt();
		}

		Map<String, Object> resultMap = createInterimResult(guardrailEngineParams, output, this.getClass().getName(),
				closeRoomOnBlock, logoutOnBlock, blockErrorMessage, cannedResponse);

		// Update the processedArguments with the interim result
		Map<String, Object> processedArguments = helper.getArgumentsMap();
		processedArguments.put(PipelineReactorUtils.INTERIM_RESULT, resultMap);
		return new NounMetadata(processedArguments, PixelDataType.MAP);
	}

	/**
	 * Reads a mapped argument, unwrapping a wrapped return value so the guardrail
	 * receives the payload rather than the wrapper. A dot path continues into that
	 * payload, so {@code result.url} reads inside a map shaped payload even though
	 * the wrapper itself is not a map.
	 *
	 * @param helper  reader for the intercepted call's arguments
	 * @param argName argument name, optionally followed by a dot path
	 * @return the value for the guardrail, or null when the path does not exist
	 */
	static Object resolveArgument(ReactorInputHelper helper, String argName) {
		Object direct = helper.getMethodArgument(argName);
		if (direct != null) {
			return GuardrailValueReader.screenableValue(direct);
		}

		// A response object is not a map, so the helper cannot walk a path through
		// it. Resolve the root on its own and read the rest from the payload.
		int split = argName == null ? -1 : argName.indexOf('.');
		if (split <= 0) {
			return null;
		}
		Object root = helper.getMethodArgument(argName.substring(0, split));
		if (!(root instanceof AbstractModelEngineResponse)) {
			return null;
		}
		return ReactorInputHelper.resolveValuePath(((AbstractModelEngineResponse<?>) root).getResponse(),
				argName.substring(split + 1));
	}

	/**
	 * Builds the interim result the invocation handler reads to decide whether the
	 * response is allowed through, and to audit what the guardrail saw.
	 *
	 * @param guardrailEngineParams values handed to the guardrail engine
	 * @param output                the guardrail engine's verdict
	 * @param interceptorName       class name recorded on the audit row
	 * @param closeRoomOnBlock      whether a block also closes the room
	 * @param blockErrorMessage     message returned in place of the response
	 * @return the interim result map
	 */
	private Map<String, Object> createInterimResult(Map<String, Object> guardrailEngineParams,
			GuardrailNounMetadata output, String interceptorName, Boolean closeRoomOnBlock, Boolean logoutOnBlock,
			String blockErrorMessage, String cannedResponse) {
		Map<String, Object> resultMap = new HashMap<>();
		resultMap.put(PipelineReactorUtils.INTERCEPTOR, interceptorName);
		resultMap.put(RETURN_PROMPT_KEY, output.getReturnPrompt());
		resultMap.put(FULL_DETAILS_KEY, output.getFullDetails());
		resultMap.put("guardrailEngineParams", guardrailEngineParams);
		resultMap.put(PipelineReactorUtils.PASS, output.isPass());
		if (Boolean.TRUE.equals(closeRoomOnBlock) && !output.isPass()) {
			resultMap.put(PipelineReactorUtils.CLOSE_ROOM, true);
		}
		if (Boolean.TRUE.equals(logoutOnBlock) && !output.isPass() && cannedResponse == null) {
			resultMap.put(PipelineReactorUtils.LOGOUT_USER, true);
		}
		if (blockErrorMessage != null && !blockErrorMessage.isEmpty()) {
			resultMap.put(PipelineReactorUtils.BLOCK_ERROR_MESSAGE, blockErrorMessage);
		}
		if (cannedResponse != null) {
			resultMap.put(PipelineReactorUtils.SHORT_CIRCUIT_RESPONSE, cannedResponse);
		}
		return resultMap;
	}
}
