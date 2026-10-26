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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.guardrail.JevPolicyGuardrailEngine.EngineDefaults;

class JevPolicyGuardrailEngineUnitTests {

	/**
	 * Builds the SMSS properties of a yes or no policy, as the guardrail import
	 * form writes them.
	 *
	 * @return the properties, which a test can change
	 */
	private static Properties noulProperties() {
		Properties props = new Properties();
		props.setProperty(JevPolicyGuardrailEngine.MODEL_ENGINE_ID_KEY, "judge-model");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_TYPE_KEY, "NOUL");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_INSTRUCTIONS_KEY,
				"Is this request within the approved scope?");
		props.setProperty(JevPolicyGuardrailEngine.VIOLATION_DIRECTION_KEY, "NO");
		props.setProperty(JevPolicyGuardrailEngine.CONFIDENCE_THRESHOLD_KEY, "0.7");
		props.setProperty(JevPolicyGuardrailEngine.LOW_CONFIDENCE_VERDICT_KEY, "PASS");
		props.setProperty(JevPolicyGuardrailEngine.JUDGE_ERROR_VERDICT_KEY, "BLOCK");
		return props;
	}

	@Test
	void readsAYesOrNoPolicy() {
		EngineDefaults defaults = JevPolicyGuardrailEngine.fromProperties(noulProperties());

		assertEquals("judge-model", defaults.judgeEngineId);
		assertEquals("NOUL", defaults.questionType);
		assertFalse(defaults.yesViolates);
		assertEquals(0.7, defaults.confidenceThreshold);
		assertEquals(GuardrailVerdict.PASS, defaults.lowConfidenceVerdict);
		assertEquals(GuardrailVerdict.BLOCK, defaults.judgeErrorVerdict);
		assertNull(defaults.blockedMessage);
		assertNull(defaults.timeoutSeconds);
	}

	@Test
	void blocksOnAJudgeErrorOrALowConfidenceAnswerUnlessTold() {
		Properties props = noulProperties();
		props.remove(JevPolicyGuardrailEngine.LOW_CONFIDENCE_VERDICT_KEY);
		props.remove(JevPolicyGuardrailEngine.JUDGE_ERROR_VERDICT_KEY);

		EngineDefaults defaults = JevPolicyGuardrailEngine.fromProperties(props);

		assertEquals(GuardrailVerdict.BLOCK, defaults.lowConfidenceVerdict);
		assertEquals(GuardrailVerdict.BLOCK, defaults.judgeErrorVerdict);
	}

	@Test
	void refusesAJudgeErrorVerdictThatIsNotBlockOrPass() {
		Properties props = noulProperties();
		props.setProperty(JevPolicyGuardrailEngine.JUDGE_ERROR_VERDICT_KEY, "true");

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.fromProperties(props));
		assertTrue(error.getMessage().startsWith(JevPolicyGuardrailEngine.JUDGE_ERROR_VERDICT_KEY));
	}

	@Test
	void letsOneCallChooseItsJudgeErrorVerdict() {
		EngineDefaults defaults = JevPolicyGuardrailEngine.fromProperties(noulProperties());

		assertEquals(GuardrailVerdict.PASS,
				JevPolicyGuardrailEngine.resolveCallConfig(
						Map.of(JevPolicyGuardrailEngine.JUDGE_ERROR_VERDICT_PARAM, "pass"),
						defaults).judgeErrorVerdict);
		assertEquals(GuardrailVerdict.BLOCK,
				JevPolicyGuardrailEngine.resolveCallConfig(Map.of(), defaults).judgeErrorVerdict);
	}

	@Test
	void treatsABlankTimeoutAsNoTimeout() {
		Properties props = noulProperties();
		props.setProperty(JevPolicyGuardrailEngine.TIMEOUT_SECONDS_KEY, "");

		assertNull(JevPolicyGuardrailEngine.fromProperties(props).timeoutSeconds);
	}

	@Test
	void readsATimeoutAndRefusesOneThatIsNotPositive() {
		Properties props = noulProperties();
		props.setProperty(JevPolicyGuardrailEngine.TIMEOUT_SECONDS_KEY, "30");
		assertEquals(30.0, JevPolicyGuardrailEngine.fromProperties(props).timeoutSeconds);

		props.setProperty(JevPolicyGuardrailEngine.TIMEOUT_SECONDS_KEY, "0");
		assertThrows(IllegalArgumentException.class, () -> JevPolicyGuardrailEngine.fromProperties(props));
	}

	@Test
	void readsAChoicePolicyWithItsBlockingChoices() {
		Properties props = noulProperties();
		props.remove(JevPolicyGuardrailEngine.VIOLATION_DIRECTION_KEY);
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_TYPE_KEY, "CHOICE");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_CRITERIA_KEY,
				"{\"APPROVED\":\"shares approved figures\",\"OFF_PLAN\":\"mentions figures not on the approved list\"}");
		props.setProperty(JevPolicyGuardrailEngine.VIOLATION_CHOICES_KEY, "[\"OFF_PLAN\"]");

		EngineDefaults defaults = JevPolicyGuardrailEngine.fromProperties(props);

		assertEquals("CHOICE", defaults.questionType);
		assertEquals(
				Map.of("APPROVED", "shares approved figures", "OFF_PLAN", "mentions figures not on the approved list"),
				defaults.criteria);
		assertEquals(List.of("OFF_PLAN"), List.copyOf(defaults.violationChoices));
	}

	@Test
	void refusesABlockingChoiceThatIsNotOneOfTheChoices() {
		Properties props = noulProperties();
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_TYPE_KEY, "CHOICE");
		props.setProperty(JevPolicyGuardrailEngine.QUESTION_CRITERIA_KEY, "{\"APPROVED\":\"shares approved figures\"}");
		props.setProperty(JevPolicyGuardrailEngine.VIOLATION_CHOICES_KEY, "[\"OFF_PLAN\"]");

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> JevPolicyGuardrailEngine.fromProperties(props));
		assertTrue(error.getMessage().contains("OFF_PLAN"));
	}
}
