/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 *
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 *
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;

class JevPolicyGuardrailEngineUnitTests {

	private static final String CRITERIA_JSON = "{\"APPROVED\":\"share approved\",\"OFF_PLAN\":\"share off plan\"}";
	private static final Set<String> VIOLATION_CHOICES = Set.of("OFF_PLAN");

	@AfterEach
	void clearRecursionTracking() {
		JevPolicyGuardrailEngine.clearActive("guardrail-id");
	}

	private static Properties noulProperties() {
		Properties props = new Properties();
		props.setProperty(JevPolicyGuardrailEngine.MODEL_ENGINE_ID_KEY, "jev-engine-id");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_INSTRUCTIONS_KEY,
				"Is this request outside the approved scope?");
		return props;
	}

	private static TypeSafeModelEngineResponse noulResponse(double probability) {
		Map<String, Object> answers = Map.of("policy", Map.of("noul", probability));
		return response(Map.of("model", "jev-latest", "answers", answers, "usage", Map.of()));
	}

	private static TypeSafeModelEngineResponse choiceResponse(String choice, double confidence) {
		Map<String, Object> answers = Map.of("policy", Map.of("choice", choice, "confidence", confidence));
		return response(Map.of("model", "jev-latest", "answers", answers, "usage", Map.of()));
	}

	private static TypeSafeModelEngineResponse response(Map<String, Object> raw) {
		// mirror the shape TypeSafeModelEngineResponse.fromObject preserves
		Map<String, Object> full = new LinkedHashMap<>(raw);
		full.put("usage", new LinkedHashMap<>(Map.of("input_tokens", 1, "output_tokens", 1)));
		return TypeSafeModelEngineResponse.fromObject(full);
	}

	private static JevPolicyGuardrailEngine.CallConfig noulConfig() throws Exception {
		return JevPolicyGuardrailEngine.resolveCallConfig(Map.of(),
				JevPolicyGuardrailEngine.fromProperties(noulProperties()));
	}

	private static JevPolicyGuardrailEngine.CallConfig choiceConfig() throws Exception {
		Properties props = new Properties();
		props.setProperty(JevPolicyGuardrailEngine.MODEL_ENGINE_ID_KEY, "jev-engine-id");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_TYPE_KEY, "CHOICE");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_INSTRUCTIONS_KEY, "Classify the response");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_CRITERIA_KEY, CRITERIA_JSON);
		props.setProperty(JevPolicyGuardrailEngine.VIOLATION_CHOICES_KEY, "[\"OFF_PLAN\"]");
		return JevPolicyGuardrailEngine.resolveCallConfig(Map.of(),
				JevPolicyGuardrailEngine.fromProperties(props));
	}

	// ---- open/validation ----------------------------------------------------

	@Test
	void fromPropertiesRequiresTheJudgeAndTheCriterion() {
		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.fromProperties(new Properties()));

		Properties withoutInstructions = noulProperties();
		withoutInstructions.remove(JevPolicyGuardrailEngine.QUESTION_INSTRUCTIONS_KEY);
		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.fromProperties(withoutInstructions));

		assertEquals("jev-engine-id",
				JevPolicyGuardrailEngine.fromProperties(noulProperties()).judgeEngineId);
	}

	@Test
	void noulDefaultsAreFailClosedBlockOnLowConfidence() throws Exception {
		JevPolicyGuardrailEngine.EngineDefaults defaults = JevPolicyGuardrailEngine
				.fromProperties(noulProperties());
		assertEquals("NOUL", defaults.questionType);
		assertTrue(defaults.yesViolates);
		assertEquals(0.5, defaults.confidenceThreshold);
		assertTrue(defaults.blockOnLowConfidence);
		assertFalse(defaults.failOpen);
	}

	@Test
	void choiceConfigurationRequiresCriteriaAndViolationChoices() {
		Properties missingCriteria = new Properties();
		missingCriteria.setProperty(JevPolicyGuardrailEngine.MODEL_ENGINE_ID_KEY, "jev-engine-id");
		missingCriteria.setProperty(JevPolicyGuardrailEngine.QUESTION_TYPE_KEY, "CHOICE");
		missingCriteria.setProperty(JevPolicyGuardrailEngine.QUESTION_INSTRUCTIONS_KEY, "classify");
		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.fromProperties(missingCriteria));
	}

	@Test
	void violationChoicesMustBeConfiguredChoices() {
		Properties props = new Properties();
		props.setProperty(JevPolicyGuardrailEngine.MODEL_ENGINE_ID_KEY, "jev-engine-id");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_TYPE_KEY, "CHOICE");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_INSTRUCTIONS_KEY, "classify");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_CRITERIA_KEY, CRITERIA_JSON);
		props.setProperty(JevPolicyGuardrailEngine.VIOLATION_CHOICES_KEY, "[\"NOT_A_CHOICE\"]");
		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.fromProperties(props));
	}

	@Test
	void invalidThresholdAndBooleanAndTimeoutAreRejected() {
		Properties badThreshold = noulProperties();
		badThreshold.setProperty(JevPolicyGuardrailEngine.CONFIDENCE_THRESHOLD_KEY, "1.5");
		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.fromProperties(badThreshold));

		Properties badFailOpen = noulProperties();
		badFailOpen.setProperty(JevPolicyGuardrailEngine.FAIL_OPEN_KEY, "TRUE ");
		// tolerant parse is intentional: booleans accept case and surrounding space
		assertTrue(JevPolicyGuardrailEngine.fromProperties(badFailOpen).failOpen);

		Properties badTimeout = noulProperties();
		badTimeout.setProperty(JevPolicyGuardrailEngine.TIMEOUT_SECONDS_KEY, "0");
		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.fromProperties(badTimeout));
	}

	@Test
	void scoreQuestionsAreRejected() {
		Properties props = noulProperties();
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_TYPE_KEY, "SCORE");
		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.fromProperties(props));
	}

	// ---- noul decision mapping ----------------------------------------------

	@Test
	void yesByDefaultBlocksAtOrAboveHalfProbability() throws Exception {
		JevPolicyGuardrailEngine.CallConfig call = noulConfig();

		JevPolicyGuardrailEngine.Decision atBoundary = JevPolicyGuardrailEngine
				.decideNoul(Map.of("noul", 0.5), call);
		assertTrue(atBoundary.violated);
		assertTrue(atBoundary.determined);

		JevPolicyGuardrailEngine.Decision belowBoundary = JevPolicyGuardrailEngine
				.decideNoul(Map.of("noul", 0.49), call);
		assertFalse(belowBoundary.violated);
		assertTrue(belowBoundary.determined);

		JevPolicyGuardrailEngine.Decision clearYes = JevPolicyGuardrailEngine
				.decideNoul(Map.of("noul", 0.97), call);
		assertTrue(clearYes.violated);
		assertTrue(clearYes.determined);
		assertEquals(0.97, clearYes.confidence);
	}

	@Test
	void noDirectionInvertsWhichSideBlocks() throws Exception {
		Properties props = noulProperties();
		props.setProperty(JevPolicyGuardrailEngine.VIOLATION_DIRECTION_KEY, "NO");
		JevPolicyGuardrailEngine.CallConfig call = JevPolicyGuardrailEngine.resolveCallConfig(Map.of(),
				JevPolicyGuardrailEngine.fromProperties(props));

		assertTrue(JevPolicyGuardrailEngine.decideNoul(Map.of("noul", 0.4), call).violated);
		assertFalse(JevPolicyGuardrailEngine.decideNoul(Map.of("noul", 0.6), call).violated);
	}

	@Test
	void belowThresholdIsIndeterminateNotAViolation() throws Exception {
		Properties props = noulProperties();
		props.setProperty(JevPolicyGuardrailEngine.CONFIDENCE_THRESHOLD_KEY, "0.9");
		JevPolicyGuardrailEngine.CallConfig call = JevPolicyGuardrailEngine.resolveCallConfig(Map.of(),
				JevPolicyGuardrailEngine.fromProperties(props));

		JevPolicyGuardrailEngine.Decision decision = JevPolicyGuardrailEngine
				.decideNoul(Map.of("noul", 0.55), call);
		assertTrue(decision.violated);
		// determined but under the confidence bar
		assertFalse(decision.determined);
		assertEquals(0.55, decision.confidence);
	}

	@Test
	void malformedNoulAnswersAreErrors() throws Exception {
		JevPolicyGuardrailEngine.CallConfig call = noulConfig();

		assertTrue(JevPolicyGuardrailEngine.decideNoul(Map.of(), call).errored);
		assertTrue(JevPolicyGuardrailEngine.decideNoul(Map.of("noul", "high"), call).errored);
		assertTrue(JevPolicyGuardrailEngine.decideNoul(Map.of("noul", 1.5), call).errored);
		assertTrue(JevPolicyGuardrailEngine.decideNoul(Map.of("noul", -0.1), call).errored);
	}

	@Test
	void decideRejectsAResponseWithoutThePolicyAnswer() throws Exception {
		JevPolicyGuardrailEngine.CallConfig call = noulConfig();

		// answers present, but no entry for the named question
		TypeSafeModelEngineResponse otherQuestion = response(
				Map.of("model", "m", "answers", Map.of("route", Map.of("noul", 0.9)), "usage", Map.of()));
		assertTrue(JevPolicyGuardrailEngine.decide(otherQuestion, call).errored);
	}

	// ---- choice decision mapping ---------------------------------------------

	@Test
	void violatingChoiceBlocksWhenConfident() throws Exception {
		JevPolicyGuardrailEngine.CallConfig call = choiceConfig();

		assertTrue(JevPolicyGuardrailEngine.decideChoice(Map.of("choice", "OFF_PLAN", "confidence", 0.8), call).violated);
		assertFalse(JevPolicyGuardrailEngine.decideChoice(Map.of("choice", "APPROVED", "confidence", 0.8), call).violated);

		// unknown choices and malformed confidences are errors, never passes
		assertTrue(JevPolicyGuardrailEngine.decideChoice(Map.of("choice", "MADE_UP", "confidence", 0.9), call).errored);
		assertTrue(JevPolicyGuardrailEngine.decideChoice(Map.of("choice", "OFF_PLAN", "confidence", "high"), call).errored);
		assertTrue(JevPolicyGuardrailEngine.decideChoice(Map.of("confidence", 0.9), call).errored);
	}

	@Test
	void choiceBelowThresholdIsIndeterminate() throws Exception {
		Properties props = new Properties();
		props.setProperty(JevPolicyGuardrailEngine.MODEL_ENGINE_ID_KEY, "jev-engine-id");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_TYPE_KEY, "CHOICE");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_INSTRUCTIONS_KEY, "classify");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_CRITERIA_KEY, CRITERIA_JSON);
		props.setProperty(JevPolicyGuardrailEngine.VIOLATION_CHOICES_KEY, "[\"OFF_PLAN\"]");
		props.setProperty(JevPolicyGuardrailEngine.CONFIDENCE_THRESHOLD_KEY, "0.9");
		JevPolicyGuardrailEngine.CallConfig call = JevPolicyGuardrailEngine.resolveCallConfig(Map.of(),
				JevPolicyGuardrailEngine.fromProperties(props));

		JevPolicyGuardrailEngine.Decision decision = JevPolicyGuardrailEngine
				.decideChoice(Map.of("choice", "OFF_PLAN", "confidence", 0.75), call);
		assertTrue(decision.violated);
		assertFalse(decision.determined);
	}

	// ---- call overrides -------------------------------------------------------

	@Test
	void overridesReplaceEngineDefaultsPerCall() throws Exception {
		Map<String, String> keyValue = Map.of(
				JevPolicyGuardrailEngine.QUESTION_TYPE_PARAM, "CHOICE",
				JevPolicyGuardrailEngine.QUESTION_CRITERIA_PARAM, CRITERIA_JSON,
				JevPolicyGuardrailEngine.VIOLATION_CHOICES_PARAM, "[\"OFF_PLAN\"]",
				JevPolicyGuardrailEngine.CONFIDENCE_THRESHOLD_PARAM, "0.8",
				JevPolicyGuardrailEngine.FAIL_OPEN_PARAM, "true",
				JevPolicyGuardrailEngine.BLOCKED_MESSAGE_PARAM, "blocked!",
				JevPolicyGuardrailEngine.QUESTION_INSTRUCTIONS_PARAM, "per-call criterion");

		JevPolicyGuardrailEngine.CallConfig call = JevPolicyGuardrailEngine
				.resolveCallConfig(keyValue, JevPolicyGuardrailEngine.fromProperties(noulProperties()));

		assertEquals("CHOICE", call.questionType);
		assertEquals(0.8, call.confidenceThreshold);
		assertTrue(call.failOpen);
		assertEquals("blocked!", call.blockedMessage);
		assertEquals(Map.of("APPROVED", "share approved", "OFF_PLAN", "share off plan"), call.criteria);
		assertEquals(VIOLATION_CHOICES, call.violationChoices);
	}

	@Test
	void invalidOverridesBecomeErrorsNotSilentDefaults() throws Exception {
		JevPolicyGuardrailEngine.EngineDefaults defaults = JevPolicyGuardrailEngine
				.fromProperties(noulProperties());

		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.resolveCallConfig(
						Map.of(JevPolicyGuardrailEngine.QUESTION_TYPE_PARAM, "SCORE"), defaults));
		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.resolveCallConfig(
						Map.of(JevPolicyGuardrailEngine.CONFIDENCE_THRESHOLD_PARAM, "2"), defaults));
		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.resolveCallConfig(
						Map.of(JevPolicyGuardrailEngine.FAIL_OPEN_PARAM, "maybe"), defaults));

		// a CHOICE override without criteria cannot resolve and must not fall
		// back to the engine's noul shape silently
		assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.resolveCallConfig(
						Map.of(JevPolicyGuardrailEngine.QUESTION_TYPE_PARAM, "CHOICE"), defaults));
	}

	// ---- recursion prevention --------------------------------------------------

	@Test
	void recursionMarkerTracksSingleThreadMembership() {
		assertFalse(JevPolicyGuardrailEngine.isActive("guardrail-id"));
		JevPolicyGuardrailEngine.markActive("guardrail-id");
		assertTrue(JevPolicyGuardrailEngine.isActive("guardrail-id"));
		JevPolicyGuardrailEngine.clearActive("guardrail-id");
		assertFalse(JevPolicyGuardrailEngine.isActive("guardrail-id"));
	}

	// ---- documentation ---------------------------------------------------------

	@Test
	void defaultMarkdownDocumentsTwoPoliciesWithTheSameEngine() {
		JevPolicyGuardrailEngine engine = new JevPolicyGuardrailEngine();
		engine.setEngineId("jev-guardrail-id");
		String markdown = engine.getDefaultMarkdown();

		assertTrue(markdown.contains("\"guardrailEngineId\": \"jev-guardrail-id\""));
		assertTrue(markdown.contains("askRoom"));
		assertTrue(markdown.contains("QUESTION_INSTRUCTIONS"));
		assertTrue(markdown.contains("approved scope"));
		assertTrue(markdown.contains("disclosure"));
	}
}