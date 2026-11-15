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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

class AggressiveSelfHarmGuardrailEngineUnitTests {

	private static final String JUDGE_ID = "judge-model";
	private static final String PROMPT = "I had a rough day";

	@TempDir
	Path engineDir;

	@Test
	void reportsTheTypeTheCatalogCreatesItUnder() throws Exception {
		try (AggressiveSelfHarmGuardrailEngine engine = new AggressiveSelfHarmGuardrailEngine()) {
			assertEquals(GuardrailTypeEnum.EMBEDDED_AGGRESSIVE_SELF_HARM, engine.getGuardrailType());
		}
	}

	@Test
	void anUnsafeAnswerBlocksTheCallWithTheCrisisMessage() throws Exception {
		GuardrailNounMetadata result = judgeAnswering("UNSAFE", null);

		assertFalse(result.isPass());
		assertTrue(result.getReturnPrompt().contains("988"), result.getReturnPrompt());
		assertEquals("UNSAFE", details(result).get("classification"));
		assertEquals(JUDGE_ID, details(result).get("modelEngineId"));
	}

	@Test
	void aSafeAnswerPassesThePromptThrough() throws Exception {
		GuardrailNounMetadata result = judgeAnswering("SAFE", null);

		assertTrue(result.isPass());
		assertEquals(PROMPT, result.getReturnPrompt());
	}

	@Test
	void anAnswerThatExplainsItselfBlocksWhenItNamesUnsafe() throws Exception {
		assertFalse(judgeAnswering("This message is UNSAFE because it describes self harm.", null).isPass());
		assertFalse(judgeAnswering("unsafe", null).isPass());
		assertTrue(judgeAnswering("safe", null).isPass());
	}

	@Test
	void aBlockedMessageFromTheSmssReplacesTheCrisisMessage() throws Exception {
		Properties smss = smss(null);
		smss.setProperty(PromptGuardrailEngine.BLOCKED_MESSAGE_KEY, "Please reach out to the help desk.");

		GuardrailNounMetadata result = judge(smss, roomAnswering("UNSAFE"));

		assertFalse(result.isPass());
		assertEquals("Please reach out to the help desk.", result.getReturnPrompt());
	}

	@Test
	void anAnswerNamingNeitherVerdictBlocksByDefault() throws Exception {
		GuardrailNounMetadata result = judgeAnswering("I cannot answer that", null);

		assertFalse(result.isPass());
		assertEquals("BLOCK", details(result).get("judgeErrorVerdict"));
	}

	@Test
	void anAnswerNamingNeitherVerdictFollowsTheJudgeErrorVerdict() throws Exception {
		GuardrailNounMetadata passed = judgeAnswering("", "PASS");
		assertTrue(passed.isPass());
		assertEquals(PROMPT, passed.getReturnPrompt());
		assertEquals("PASS", details(passed).get("judgeErrorVerdict"));

		assertFalse(judgeAnswering("", "BLOCK").isPass());
	}

	@Test
	void aVerdictTheJudgeNamesIsNotOverriddenByTheJudgeErrorVerdict() throws Exception {
		GuardrailNounMetadata result = judgeAnswering("UNSAFE", "PASS");

		assertFalse(result.isPass());
		assertNull(details(result).get("judgeErrorVerdict"));
	}

	@Test
	void aJudgeThatFailsFollowsTheJudgeErrorVerdict() throws Exception {
		GuardrailNounMetadata blocked = judgeFailing(null);
		assertFalse(blocked.isPass());
		assertEquals("ERROR", details(blocked).get("classification"));
		assertEquals("The judge model is unavailable", details(blocked).get("error"));

		assertTrue(judgeFailing("PASS").isPass());
	}

	@Test
	void aMissingJudgeModelFollowsTheJudgeErrorVerdict() throws Exception {
		GuardrailNounMetadata result = judge(smss(null), null);

		assertFalse(result.isPass());
		assertEquals("ERROR", details(result).get("classification"));
	}

	@Test
	void openRequiresAJudgeModel() {
		Properties smss = smss(null);
		smss.remove(PromptGuardrailEngine.MODEL_ENGINE_ID_KEY);

		assertThrows(IllegalArgumentException.class, () -> judge(smss, roomAnswering("SAFE")));
	}

	private GuardrailNounMetadata judgeAnswering(String answer, String judgeErrorVerdict) throws Exception {
		return judge(smss(judgeErrorVerdict), roomAnswering(answer));
	}

	private GuardrailNounMetadata judgeFailing(String judgeErrorVerdict) throws Exception {
		Room room = room();
		doThrow(new IllegalStateException("The judge model is unavailable")).when(room)
				.ask(any(InputMessage.class), any(IModelEngine.class));
		return judge(smss(judgeErrorVerdict), room);
	}

	private static Room roomAnswering(String answer) {
		AskModelEngineResponse<?> modelResponse = mock(AskModelEngineResponse.class);
		when(modelResponse.toMap()).thenReturn(Map.of("response", answer));
		ResponseMessage response = mock(ResponseMessage.class);
		doReturn(modelResponse).when(response).getModelEngineResponse();

		Room room = room();
		doReturn(response).when(room).ask(any(InputMessage.class), any(IModelEngine.class));
		return room;
	}

	private static Room room() {
		Room room = spy(new Room());
		room.setId("room-id");
		return room;
	}

	private static Properties smss(String judgeErrorVerdict) {
		Properties smss = new Properties();
		smss.setProperty(Constants.ENGINE, "self-harm-id");
		smss.setProperty(Constants.ENGINE_ALIAS, "Self Harm");
		smss.setProperty(PromptGuardrailEngine.MODEL_ENGINE_ID_KEY, JUDGE_ID);
		if (judgeErrorVerdict != null) {
			smss.setProperty(PromptGuardrailEngine.JUDGE_ERROR_VERDICT_KEY, judgeErrorVerdict);
		}
		return smss;
	}

	private static Map<?, ?> details(GuardrailNounMetadata result) {
		return (Map<?, ?>) result.getFullDetails();
	}

	/**
	 * Opens the guardrail in a temporary engine folder and judges {@link #PROMPT}.
	 * The judge model resolves only when {@code room} is given.
	 */
	private GuardrailNounMetadata judge(Properties smss, Room room) throws Exception {
		String folder = this.engineDir.toString();
		try (MockedStatic<SecretsFactory> secrets = mockStatic(SecretsFactory.class);
				MockedStatic<EngineUtility> folders = mockStatic(EngineUtility.class);
				MockedStatic<GitRepoUtils> git = mockStatic(GitRepoUtils.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<RoomUtils> rooms = mockStatic(RoomUtils.class);
				AggressiveSelfHarmGuardrailEngine engine = new AggressiveSelfHarmGuardrailEngine()) {
			folders.when(() -> EngineUtility.getSpecificEngineBaseFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineAppRootFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineVersionFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(), any())).thenReturn(folder);
			if (room != null) {
				utility.when(() -> Utility.getModel(JUDGE_ID)).thenReturn(mock(IModelEngine.class));
				rooms.when(() -> RoomUtils.createRoomIfNotExists(anyString(), any(), any(), anyString()))
						.thenReturn(room);
			}

			engine.open(smss);
			NounStore ns = new NounStore("all");
			ns.makeGenRowStruct("prompt").add(new NounMetadata(PROMPT, PixelDataType.CONST_STRING));
			return engine.execute(ns, new GenRowStruct());
		}
	}
}
