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

import prerna.engine.api.IEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.ToolResultMessagePart;
import prerna.engine.impl.model.message.ToolResultPart;
import prerna.engine.impl.model.message.InputMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Screens the arguments of an intercepted engine method before it runs. On
 * failure the call is blocked, or the guarded argument is replaced with the
 * guardrail's masked text, or the guardrail's message is returned in place of
 * calling the method at all.
 *
 * Any engine type can attach this to a pipeline slot. The intercepted method's
 * arguments are exposed under the names the runtime resolves for them, and a
 * mapped argument that wraps its text is unwrapped before the guardrail sees
 * it, so a guardrail always receives content rather than an object.
 */
public class GenericGuardrailInputReactor extends AbstractReactor implements IInputReactor {

	private static final Logger classLogger = LogManager.getLogger(GenericGuardrailInputReactor.class);

	// default guardrail input param whose mapped argument gets overwritten when masking
	private static final String DEFAULT_MASK_TARGET_PARAM = "prompt";
	private static final String DEFAULT_TOOL_CONTINUATION_ARG = "arg2";
	private transient IEngine targetEngine;

	public GenericGuardrailInputReactor() {
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
					"GenericGuardrailInputReactor is not configured correctly. Missing 'guardrailEngineId'.");
		}
		IGuardrailReactorFunctionEngine guardrailEngine = Utility.getGuardrailEngine(guardrailEngineId);
		if (guardrailEngine == null) {
			throw new SecurityException("Guardrail engine with ID '" + guardrailEngineId + "' not found.");
		}

		// Some mounts (e.g. prompt-injection classifiers tuned on user-typed text) should
		// only ever see real user turns - askRoom is reused for tool-result continuations,
		// so skip this mount when the guarded argument is a tool result from a listed tool.
		if (isToolContinuation(helper)) {
			Map<String, Object> processedArguments = helper.getArgumentsMap();
			processedArguments.put(PipelineReactorUtils.INTERIM_RESULT, createSkippedInterimResult());
			return new NounMetadata(processedArguments, PixelDataType.MAP);
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
		// The values that came from the intercepted call, kept apart from the
		// configured direct parameters so a canned response can be compared
		// against what the guardrail was actually given to screen
		List<Object> mappedInputValues = new ArrayList<>();

		// Process the inputMapping to get parameters from the intercepted method's
		// arguments
		for (Map.Entry<String, Object> entry : inputMapping.entrySet()) {
			String guardrailParamName = entry.getKey();
			Object mappedValue = entry.getValue();

			if (mappedValue instanceof String) {
				Object argValue = resolveArgument(helper, (String) mappedValue);
				guardrailEngineParams.put(guardrailParamName, argValue);
				mappedInputValues.add(argValue);
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
					String combined = combinedPrompt.toString().trim();
					guardrailEngineParams.put(guardrailParamName, combined);
					mappedInputValues.add(combined);
				} else {
					List<Object> values = new ArrayList<>();
					for (Object name : argNames) {
						if (name instanceof String) {
							values.add(resolveArgument(helper, (String) name));
						}
					}
					guardrailEngineParams.put(guardrailParamName, values);
					mappedInputValues.add(values);
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

		// Keep the actual engine out of all maps and nouns.
		GuardrailNounMetadata output = guardrailEngine.execute(guardrailInputNounStore, null, this.targetEngine);

		Map<String, Object> processedArguments = helper.getArgumentsMap();

		// If this filter is configured to mask (rather than block) on failure,
		// overwrite the guarded argument with the masked text the guardrail
		// produced, so the masked text - not the original - is what the
		// intercepted method receives.
		boolean masked = false;
		Boolean maskOnGuardrailFailure = helper.getConfigParameter("maskOnGuardrailFailure", Boolean.class);
		if (maskOnGuardrailFailure != null && maskOnGuardrailFailure && !output.isPass()) {
			if (replaceGuardedInput(helper, inputMapping, output.getReturnPrompt())) {
				masked = true;
			} else {
				// cannot safely write the masked value back (no single text argument
				// to write to, more than one candidate, or an unresolvable path) -
				// leave pass as-is so the call is blocked rather than letting
				// unmasked content through
				classLogger.warn(
						"maskOnGuardrailFailure is enabled but no single text argument could be identified to receive "
								+ "the masked value; blocking instead of masking.");
			}
		}

		// When configured to respond (rather than mask or block), hand the guardrail's
		// message back as the call's return value. The intercepted method is skipped
		// entirely, so no version of the guarded input reaches it.
		String cannedResponse = null;
		Boolean respondWithGuardrailMessage = helper.getConfigParameter("respondWithGuardrailMessage", Boolean.class);
		if (Boolean.TRUE.equals(respondWithGuardrailMessage) && !output.isPass()) {
			String candidate = output.getReturnPrompt();
			// unchanged against any screened input means the guardrail produced no
			// message of its own, so there is nothing to answer with
			if (candidate != null && !mappedInputValues.contains(candidate)) {
				cannedResponse = candidate;
			} else {
				classLogger.warn("respondWithGuardrailMessage is enabled but guardrail engine '{}' returned the input "
						+ "unchanged; blocking instead of responding.", guardrailEngineId);
			}
		}

		Boolean closeRoomOnBlock = helper.getConfigParameter("closeRoomOnBlock", Boolean.class);
		String blockErrorMessage = helper.getConfigParameter("blockErrorMessage", String.class);

		Map<String, Object> resultMap = createInterimResult(output, this.getClass().getName(), masked, cannedResponse,
				closeRoomOnBlock, blockErrorMessage);

		// Update the processedArguments with the interim result
		processedArguments.put(PipelineReactorUtils.INTERIM_RESULT, resultMap);
		return new NounMetadata(processedArguments, PixelDataType.MAP);
	}

	/**
	 * Reads a mapped argument, reduced to the content a guardrail screens. This
	 * reduction is keyed on the argument's type rather than on the guardrail
	 * parameter's name, since a guardrail declares its own parameter names and any
	 * of them may be pointed at an argument that carries its text inside an object.
	 *
	 * @param helper  reader for the intercepted call's arguments
	 * @param argName argument name, optionally followed by a dot path
	 * @return the value for the guardrail, or null when the path does not exist
	 */
	static Object resolveArgument(ReactorInputHelper helper, String argName) {
		return GuardrailValueReader.screenableValue(helper.getMethodArgument(argName));
	}

	/**
	 * Writes the guardrail's masked text back to the argument that supplied it.
	 *
	 * @param helper       reader and writer for the intercepted call's arguments
	 * @param inputMapping guardrail parameter to argument mapping
	 * @param maskedPrompt the text the guardrail returned
	 * @return whether the masked text was written back
	 */
	static boolean replaceGuardedInput(ReactorInputHelper helper, Map<String, Object> inputMapping,
			String maskedPrompt) {
		if (maskedPrompt == null) {
			return false;
		}
		String argumentName = resolveMaskTarget(helper, inputMapping);
		if (argumentName == null) {
			return false;
		}

		Object guardedInput = helper.getMethodArgument(argumentName);
		Object replacement = maskedPrompt;
		if (guardedInput instanceof InputMessage) {
			InputMessage inputMessage = (InputMessage) guardedInput;
			inputMessage.setFullInputPrompt(maskedPrompt);
			replacement = inputMessage;
		}
		return helper.setMethodArgument(argumentName, replacement);
	}

	/**
	 * The argument a masked value can be written back to. The guardrail returns a
	 * single string, so the target is the one mapped argument that currently holds
	 * text: a String, or a message object carrying text. Which guardrail parameter
	 * reads it does not matter, so masking works for any engine whose call carries
	 * text worth screening.
	 *
	 * A mapping that combines several arguments is skipped, since a single returned
	 * string cannot be split back across them. Arguments holding anything other
	 * than text are skipped too, because replacing them would hand the method a
	 * value of the wrong type. More than one candidate is treated as no candidate
	 * rather than guessed at.
	 *
	 * @param helper       reader for the intercepted call's arguments
	 * @param inputMapping guardrail parameter to argument mapping
	 * @return the argument name to write to, or null when there is not exactly one
	 */
	static String resolveMaskTarget(ReactorInputHelper helper, Map<String, Object> inputMapping) {
		String target = null;
		for (Object mappedValue : inputMapping.values()) {
			if (!(mappedValue instanceof String)) {
				continue;
			}
			String argumentName = (String) mappedValue;
			Object argValue = helper.getMethodArgument(argumentName);
			if (!(argValue instanceof String) && !(argValue instanceof InputMessage)) {
				continue;
			}
			if (target != null && !target.equals(argumentName)) {
				// two different text arguments were screened and the guardrail's
				// single returned value gives no way to tell them apart
				return null;
			}
			target = argumentName;
		}
		return target;
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
		// when we masked the input we neutralized the failure, so let it pass
		// downstream
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

	/**
	 * True when the guarded argument is a tool-result continuation this mount should
	 * skip, never a real user turn.
	 *
	 * {@code skipOnToolContinuationForAllTools} (Boolean), when {@code true}, skips any
	 * tool-result continuation and ignores {@code skipOnToolContinuationForTools}
	 * entirely. Otherwise a non-empty {@code skipOnToolContinuationForTools} (List)
	 * skips only when every tool result on the message names a listed tool.
	 *
	 * Either way, the argument named by {@code toolContinuationArg} (default
	 * {@value #DEFAULT_TOOL_CONTINUATION_ARG}) must be an {@link AbstractMessage} that
	 * {@link AbstractMessage#hasToolResultPart()} - a real user turn is always screened,
	 * regardless of either flag.
	 */
	static boolean isToolContinuation(ReactorInputHelper helper) {
		boolean blanket = Boolean.TRUE
				.equals(helper.getConfigParameter("skipOnToolContinuationForAllTools", Boolean.class));

		List<?> allowedTools = helper.getConfigParameter("skipOnToolContinuationForTools", List.class);
		if (!blanket && (allowedTools == null || allowedTools.isEmpty())) {
			return false;
		}

		String argName = helper.getConfigParameter("toolContinuationArg", String.class);
		if (argName == null || argName.isEmpty()) {
			argName = DEFAULT_TOOL_CONTINUATION_ARG;
		}
		Object argValue = helper.getMethodArgument(argName);
		if (!(argValue instanceof AbstractMessage) || !((AbstractMessage) argValue).hasToolResultPart()) {
			return false; // not a tool continuation - always screen
		}

		if (blanket) {
			String guardrailEngineId = helper.getConfigParameter("guardrailEngineId", String.class);
			classLogger.warn("Guardrail '{}' skipped on a tool-result continuation "
					+ "(skipOnToolContinuationForAllTools=true, toolContinuationArg='{}').", guardrailEngineId,
					argName);
			return true; // blanket ignores the allowlist
		}

		for (MessagePart part : ((AbstractMessage) argValue).getParts()) {
			if (part instanceof ToolResultMessagePart) {
				ToolResultPart toolResult = ((ToolResultMessagePart) part).getToolResult();
				if (toolResult == null || !allowedTools.contains(toolResult.getToolName())) {
					return false;
				}
			}
		}
		return true;
	}

	private Map<String, Object> createSkippedInterimResult() {
		Map<String, Object> resultMap = new HashMap<>();
		resultMap.put(PipelineReactorUtils.INTERCEPTOR, this.getClass().getName());
		resultMap.put(PipelineReactorUtils.PASS, true);
		resultMap.put(PipelineReactorUtils.PASS_DETAILS, "Skipped: tool-result continuation, not a user prompt");
		resultMap.put(PipelineReactorUtils.MASKED, false);
		return resultMap;
	}
}
