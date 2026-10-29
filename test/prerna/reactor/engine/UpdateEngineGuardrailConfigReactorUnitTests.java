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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.interceptor.GenericGuardrailInputReactor;
import prerna.reactor.interceptor.GenericGuardrailOutputReactor;

class UpdateEngineGuardrailConfigReactorUnitTests {

	@Test
	void acceptsOneFailureAction() {
		Map<String, Object> blockParams = validParams();
		blockParams.put("blockErrorMessage", "The request was blocked by a guardrail.");
		assertDoesNotThrow(() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(blockParams)));

		Map<String, Object> responseParams = validParams();
		responseParams.put("respondWithGuardrailMessage", true);
		assertDoesNotThrow(() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(responseParams)));
	}

	@Test
	void validatesRuntimeBlockResponseOptionTypes() {
		assertInvalidParam("respondWithGuardrailMessage", "true", "must be a boolean");
		assertInvalidParam("blockErrorMessage", true, "must be a non-empty string");
		assertInvalidParam("blockErrorMessage", " ", "must be a non-empty string");
	}

	@Test
	void rejectsConflictingFailureActions() {
		Map<String, Object> params = validParams();
		params.put("blockOnGuardrailFailure", true);
		params.put("respondWithGuardrailMessage", true);

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(params)));
		assertTrue(exception.getMessage().contains("must enable exactly one failure action"));
	}

	@Test
	void rejectsBlockOnlyOptionsForAnotherFailureAction() {
		Map<String, Object> params = validParams();
		params.put("respondWithGuardrailMessage", true);
		params.put("blockErrorMessage", "Blocked.");

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(params)));
		assertTrue(exception.getMessage().contains("can only be set when blocking on failure"));
	}

	@Test
	void hitLimitsNameChecksByTheirIdAndAreSavedWithWholeNumbers() {
		Map<String, Object> params = validParams();
		params.put("checkId", "toxicity");
		// a pixel map delivers numbers as doubles
		Map<String, Object> roomLimit = Map.of("limitId", "close", "checks", List.of("toxicity"), "hits", 3.0, "scope",
				"ROOM");
		Map<String, Object> userLimit = Map.of("limitId", "lock", "checks", List.of("toxicity"), "hits", 5.0, "scope",
				"USER", "windowMinutes", 1440.0, "lockMinutes", 60.0);

		var validated = UpdateEngineGuardrailConfigReactor
				.validateConfigStructure(withLimits(config(params), List.of(roomLimit, userLimit)));

		assertEquals(List.of(Map.of("limitId", "close", "checks", List.of("toxicity"), "hits", 3, "scope", "ROOM"),
				Map.of("limitId", "lock", "checks", List.of("toxicity"), "hits", 5, "scope", "USER", "windowMinutes",
						1440, "lockMinutes", 60)),
				validated.toFile().get("hitLimits"));
	}

	@Test
	void rejectsHitLimitsThatCannotCount() {
		Map<String, Object> params = validParams();
		params.put("checkId", "toxicity");

		assertInvalidLimit(params, Map.of("limitId", "a", "checks", List.of("missing"), "hits", 1, "scope", "ROOM"),
				"which is not a check in this pipeline file");
		assertInvalidLimit(params, Map.of("limitId", "a", "checks", List.of("toxicity"), "hits", 0, "scope", "ROOM"),
				"must be a whole number from 1");
		assertInvalidLimit(params, Map.of("limitId", "a", "checks", List.of("toxicity"), "hits", 2.5, "scope", "ROOM"),
				"must be a whole number from 1");
		assertInvalidLimit(params,
				Map.of("limitId", "a", "checks", List.of("toxicity"), "hits", 1, "scope", "USER", "lockMinutes", 60),
				"windowMinutes must be a whole number");
		assertInvalidLimit(params,
				Map.of("limitId", "a", "checks", List.of("toxicity"), "hits", 1, "scope", "ROOM", "lockMinutes", 60),
				"a ROOM limit counts the room's hits");
		assertInvalidLimit(params, Map.of("limitId", "a", "checks", List.of(), "hits", 1, "scope", "ROOM"),
				"must list at least one check id");
	}

	@Test
	void rejectsACheckIdTwoChecksShare() {
		Map<String, Object> first = validParams();
		first.put("checkId", "same");
		Map<String, Object> second = validParams();
		second.put("checkId", "same");
		Map<String, Object> pipeline = Map.of("input",
				List.of(Map.of("reactorClass", GenericGuardrailInputReactor.class.getName(), "params", first),
						Map.of("reactorClass", GenericGuardrailInputReactor.class.getName(), "params", second)));

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> UpdateEngineGuardrailConfigReactor
						.validateConfigStructure(Map.of("pipelines", Map.of("askRoom", pipeline))));
		assertTrue(exception.getMessage().contains("is used by more than one check"));
	}

	@Test
	void maskingAcceptsAnyMappedParameterName() {
		Map<String, Object> params = validParams();
		params.put("blockOnGuardrailFailure", false);
		params.put("maskOnGuardrailFailure", true);
		assertDoesNotThrow(() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(params)));

		// the masked value is written back to the argument that supplied it, so the
		// guardrail's own parameter name carries no meaning here
		Map<String, Object> customName = validParams();
		customName.put("blockOnGuardrailFailure", false);
		customName.put("maskOnGuardrailFailure", true);
		customName.put("inputMapping", Map.of("content", "arg1"));
		assertDoesNotThrow(() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(customName)));
	}

	@Test
	void maskingRejectsWhenNoMappedArgumentCanReceiveTheValue() {
		Map<String, Object> combinedMapping = validParams();
		combinedMapping.put("blockOnGuardrailFailure", false);
		combinedMapping.put("maskOnGuardrailFailure", true);
		combinedMapping.put("inputMapping", Map.of("content", List.of("arg0", "arg1")));
		IllegalArgumentException combined = assertThrows(IllegalArgumentException.class,
				() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(combinedMapping)));
		assertTrue(combined.getMessage().contains("to a single argument name"));

		Map<String, Object> overriddenMapping = validParams();
		overriddenMapping.put("blockOnGuardrailFailure", false);
		overriddenMapping.put("maskOnGuardrailFailure", true);
		overriddenMapping.put("directParameters", Map.of("prompt", "fixed value"));
		IllegalArgumentException overridden = assertThrows(IllegalArgumentException.class,
				() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(overriddenMapping)));
		assertTrue(overridden.getMessage().contains("does not override"));
	}

	@Test
	void toolContinuationSkipIsOffUntilAMountAsksForIt() {
		// the settings a mount leaves out keep it screening tool-result turns
		assertDoesNotThrow(() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(validParams())));

		Map<String, Object> blanket = validParams();
		blanket.put("skipOnToolContinuationForAllTools", true);
		blanket.put("toolContinuationArg", "arg0");
		assertDoesNotThrow(() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(blanket)));

		Map<String, Object> allowlisted = validParams();
		allowlisted.put("skipOnToolContinuationForTools", List.of("a1234_search"));
		assertDoesNotThrow(() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(allowlisted)));
	}

	@Test
	void validatesToolContinuationSkipTypes() {
		assertInvalidParam("skipOnToolContinuationForAllTools", "true", "must be a boolean");
		assertInvalidParam("skipOnToolContinuationForTools", "search", "must be a list of tool names");
		assertInvalidParam("skipOnToolContinuationForTools", List.of(" "), "must contain non-empty tool names");
		assertInvalidParam("toolContinuationArg", true, "must be a non-empty string");
	}

	@Test
	void rejectsAToolContinuationArgWithoutASkip() {
		Map<String, Object> params = validParams();
		params.put("toolContinuationArg", "arg0");

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(params)));
		assertTrue(exception.getMessage().contains("can only be set when"));
	}

	@Test
	void rejectsToolContinuationSkipOnAnOutputGuardrail() {
		Map<String, Object> params = validParams();
		params.put("skipOnToolContinuationForAllTools", true);

		// a tool-result continuation is an argument, so there is nothing to skip on the
		// way out
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(outputConfig(params)));
		assertTrue(exception.getMessage().contains("skipping tool-result continuations requires an 'input' guardrail"));
	}

	private static void assertInvalidParam(String key, Object value, String expectedMessage) {
		Map<String, Object> params = validParams();
		params.put(key, value);

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> UpdateEngineGuardrailConfigReactor.validateConfigStructure(config(params)));
		assertTrue(exception.getMessage().contains(expectedMessage));
	}

	private static void assertInvalidLimit(Map<String, Object> params, Map<String, Object> limit,
			String expectedMessage) {
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> UpdateEngineGuardrailConfigReactor
						.validateConfigStructure(withLimits(config(params), List.of(limit))));
		assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
	}

	private static Map<String, Object> withLimits(Map<String, Object> config, List<Map<String, Object>> limits) {
		Map<String, Object> withLimits = new HashMap<>(config);
		withLimits.put("hitLimits", limits);
		return withLimits;
	}

	private static Map<String, Object> validParams() {
		Map<String, Object> params = new HashMap<>();
		params.put("guardrailEngineId", "guardrail-engine-id");
		params.put("inputMapping", Map.of("prompt", "arg0"));
		return params;
	}

	private static Map<String, Object> outputConfig(Map<String, Object> params) {
		Map<String, Object> guardrail = Map.of("reactorClass", GenericGuardrailOutputReactor.class.getName(), "params",
				params);
		Map<String, Object> pipeline = Map.of("output", List.of(guardrail));
		return Map.of("pipelines", Map.of("askCall", pipeline));
	}

	private static Map<String, Object> config(Map<String, Object> params) {
		Map<String, Object> guardrail = Map.of("reactorClass", GenericGuardrailInputReactor.class.getName(), "params",
				params);
		Map<String, Object> pipeline = Map.of("input", List.of(guardrail));
		return Map.of("pipelines", Map.of("askCall", pipeline));
	}
}
