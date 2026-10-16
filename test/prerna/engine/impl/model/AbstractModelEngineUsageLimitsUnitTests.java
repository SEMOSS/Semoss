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
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.responses.AskErrorModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageLimitException;
import prerna.engine.impl.model.usage.ModelUsageRequest;
import prerna.om.Insight;

/**
 * A call a usage limit refuses leaves the room as it was, so messages that were
 * never sent are not kept in it.
 */
class AbstractModelEngineUsageLimitsUnitTests {

	@Test
	void aCallTheClientRefusesLeavesTheRoomAsItWas() {
		TestEngine engine = new TestEngine(() -> {
			throw ModelUsageLimitException.doesNotFit("There is not enough left of your usage limit for this request");
		});
		Room room = room();
		AbstractMessage earlier = mock(AbstractMessage.class);
		room.getMessages().add(earlier);

		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class,
				() -> askWithFullPrompt(engine, room));
		assertTrue(e.callDoesNotFit());
		assertEquals(1, room.getMessages().size());
		assertSame(earlier, room.getMessages().get(0));
	}

	@Test
	void aCallAnsweredWithTheDoesNotFitErrorLeavesTheRoomAsItWas() {
		TestEngine engine = new TestEngine(() -> {
			AskErrorModelEngineResponse error = new AskErrorModelEngineResponse(
					"There is not enough left of your usage limit for this request", "Usage Limit", 429, "openai",
					"gpt-4o", null);
			error.setReason(AskErrorModelEngineResponse.REASON_USAGE_LIMIT_DOES_NOT_FIT);
			return error;
		});
		Room room = room();
		AbstractMessage earlier = mock(AbstractMessage.class);
		room.getMessages().add(earlier);

		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class,
				() -> askWithFullPrompt(engine, room));
		assertTrue(e.callDoesNotFit());
		assertEquals(List.of(earlier), room.getMessages());
	}

	@Test
	void anAskWithNoOutputMaximumIsBoundedForItsUsageLimits() {
		TestEngine engine = new TestEngine(() -> null);
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("message_json", "[]");
		assertEquals(ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS,
				engine.askUsageRequest(parameters).getMaxOutputTokens());
		parameters.put(AbstractModelEngine.MAX_TOKENS, 300);
		assertEquals(300L, engine.askUsageRequest(parameters).getMaxOutputTokens());
	}

	private static void askWithFullPrompt(TestEngine engine, Room room) {
		InputMessage input = mock(InputMessage.class);
		InputMessage sent = mock(InputMessage.class);
		Map<String, Object> parameters = new HashMap<>();
		parameters.put(AbstractModelEngine.FULL_PROMPT, "[{\"role\": \"user\", \"content\": \"hi\"}]");
		try (MockedStatic<MessageUtils> messages = mockStatic(MessageUtils.class);
				MockedStatic<RoomMessageStore> store = mockStatic(RoomMessageStore.class)) {
			messages.when(() -> MessageUtils.convertFullPrompt(any(), any(), any()))
					.thenAnswer(call -> new ArrayList<AbstractMessage>(List.of(sent)));
			messages.when(() -> MessageUtils.toJsonArray(anyList())).thenReturn("[]");
			engine.askRoom(input, room, parameters);
		}
	}

	private static Room room() {
		Room room = new Room();
		// no user: no limit applies, and the client's refusal is what is tested
		room.setInsight(mock(Insight.class));
		return room;
	}

	private static class TestEngine extends AbstractModelEngine {
		private final Supplier<AskModelEngineResponse> answer;

		TestEngine(Supplier<AskModelEngineResponse> answer) {
			this.answer = answer;
			setBasic(true);
		}

		@Override
		protected AskModelEngineResponse askCall(InputMessage message, Insight insight, String roomId,
				Map<String, Object> parameters) {
			return answer.get();
		}

		@Override
		protected EmbeddingsModelEngineResponse embeddingsCall(List<String> strings, Insight insight,
				Map<String, Object> parameters) {
			return null;
		}

		@Override
		public ModelTypeEnum getModelType() {
			return ModelTypeEnum.OPEN_AI;
		}

		@Override
		public void close() {
		}
	}
}
