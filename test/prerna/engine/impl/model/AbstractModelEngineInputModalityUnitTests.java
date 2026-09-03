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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import prerna.engine.api.ModelModalityEnum;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MediaMessagePart;
import prerna.engine.impl.model.message.MessageIO;
import prerna.engine.impl.model.message.MessageInputMedia;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.message.TextMessagePart;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;

class AbstractModelEngineInputModalityUnitTests {

	@Test
	void rejectsImageWhenModelOnlyAllowsTextInput() {
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.TEXT));
		InputMessage message = newMessage();
		message.addPart(new TextMessagePart("describe this"));
		message.addPart(new MediaMessagePart(MessageInputMedia.fromUrl("https://example.com/image.png")));

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> engine.validateInputModalities(List.of(), message));

		assertEquals("Model test-model does not allow IMAGE input. Configured input modalities: [TEXT]",
				exception.getMessage());
	}

	@Test
	void acceptsImageWhenModelAllowsImageInput() {
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.TEXT, ModelModalityEnum.IMAGE));
		InputMessage message = newMessage();
		message.addPart(new TextMessagePart("describe this"));
		message.addPart(new MediaMessagePart(MessageInputMedia.fromUrl("https://example.com/image.png")));

		assertDoesNotThrow(() -> engine.validateInputModalities(List.of(), message));
	}

	@Test
	void rejectsPdfWhenModelDoesNotAllowPdfInput() {
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.TEXT, ModelModalityEnum.IMAGE));
		InputMessage message = newMessage();
		message.addPart(new MediaMessagePart(pdfMedia()));

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> engine.validateInputModalities(List.of(), message));

		assertTrue(exception.getMessage().contains("does not allow PDF input"));
	}

	@Test
	void classifiesPdfUrlsAsPdfInput() {
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.TEXT, ModelModalityEnum.IMAGE));
		InputMessage message = newMessage();
		message.addPart(new MediaMessagePart(MessageInputMedia.fromUrl("https://example.com/report.pdf")));

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> engine.validateInputModalities(List.of(), message));

		assertTrue(exception.getMessage().contains("does not allow PDF input"));
	}

	@Test
	void allowsMediaWhoseModalityCannotBeDetermined() {
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.TEXT));
		InputMessage message = newMessage();
		message.addPart(new MediaMessagePart(MessageInputMedia.fromUrl("https://example.com/stream")));

		assertDoesNotThrow(() -> engine.validateInputModalities(List.of(), message));
	}

	@Test
	void allowsTextPartsForModelsWithoutTextInputModality() {
		// audio-only input models still receive the required text command and
		// platform-injected system prompts; TEXT is never a basis for rejection
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.AUDIO));
		InputMessage message = newMessage();
		message.addPart(new TextMessagePart("transcribe this"));

		assertDoesNotThrow(() -> engine.validateInputModalities(List.of(), message));
	}

	@Test
	void skipsModelOutputMessagesInHistory() {
		// a TTS model's own audio response in history is output, not caller input
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.TEXT));
		InputMessage root = newMessage();
		root.addPart(new TextMessagePart("say hello"));
		InputMessage audioResponse = newMessage();
		audioResponse.setIo(MessageIO.OUTPUT);
		audioResponse.setParentMessageId(root.getMessageId());
		audioResponse.addPart(new MediaMessagePart(audioMedia()));
		InputMessage followUp = newMessage();
		followUp.setParentMessageId(audioResponse.getMessageId());
		followUp.addPart(new TextMessagePart("say it again"));

		assertDoesNotThrow(() -> engine.validateInputModalities(List.of(root, audioResponse), followUp));
	}

	@Test
	void doesNotRestrictInputWhenMetadataDoesNotConfigureModalities() {
		TestModelEngine engine = new TestModelEngine(null);
		InputMessage message = newMessage();
		message.addPart(new MediaMessagePart(MessageInputMedia.fromUrl("https://example.com/image.png")));

		assertDoesNotThrow(() -> engine.validateInputModalities(List.of(), message));
	}

	@Test
	void ignoresUnsupportedPartsOnConversationBranchesNotSentToModel() {
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.TEXT));
		InputMessage root = newMessage();
		root.addPart(new TextMessagePart("root"));
		InputMessage imageBranch = newMessage();
		imageBranch.setParentMessageId(root.getMessageId());
		imageBranch.addPart(new MediaMessagePart(MessageInputMedia.fromUrl("https://example.com/image.png")));
		InputMessage textBranch = newMessage();
		textBranch.setParentMessageId(root.getMessageId());
		textBranch.addPart(new TextMessagePart("continue here"));

		assertDoesNotThrow(() -> engine.validateInputModalities(List.of(root, imageBranch), textBranch));
	}

	@Test
	void validatesEveryMessageOfAFullOutboundList() {
		// full-prompt payloads serialize the whole list, so validation must not
		// stop at the branch of the last (often parentless) message
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.TEXT));
		InputMessage imageTurn = newMessage();
		imageTurn.addPart(new MediaMessagePart(MessageInputMedia.fromUrl("https://example.com/image.png")));
		InputMessage textTurn = newMessage();
		textTurn.addPart(new TextMessagePart("just text"));

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> engine.validateInputModalities(List.of(imageTurn, textTurn)));

		assertTrue(exception.getMessage().contains("does not allow IMAGE input"));
	}

	@Test
	void usesFinalFullPromptInputAsCurrentTurn() {
		InputMessage earlierInput = newMessage();
		ResponseMessage response = ResponseMessage.builder().withText("response").build();
		InputMessage finalInput = newMessage();

		InputMessage result = AbstractModelEngine.requireFinalInputMessage(List.of(earlierInput, response, finalInput));

		assertSame(finalInput, result);
	}

	@Test
	void rejectsFullPromptEndingWithResponse() {
		InputMessage input = newMessage();
		ResponseMessage response = ResponseMessage.builder().withText("response").build();

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> AbstractModelEngine.requireFinalInputMessage(List.of(input, response)));

		assertEquals("Full prompt must end with an input message", exception.getMessage());
	}

	@Test
	@SuppressWarnings("deprecation")
	void legacyAskCallRoutesToInputMessageAskCall() {
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.TEXT));
		Object fullPrompt = List.of(Map.of("role", "user", "content", "question"));
		Map<String, Object> parameters = Map.of("temperature", 0.2);

		engine.askCall("question", fullPrompt, "system prompt", null, "legacy-room", parameters);

		assertEquals("question", engine.lastInputMessage.getFullInputPrompt());
		assertEquals("system prompt", engine.lastInputMessage.getSystemPrompt());
		assertEquals("legacy-room", engine.lastInputMessage.getRoomId());
		assertSame(fullPrompt, engine.lastInputMessage.getParamMap().get(AbstractModelEngine.FULL_PROMPT));
		assertSame(parameters, engine.lastHyperParameters);
	}

	@Test
	void branchValidationWithoutNewMessageCoversOnlyTheCurrentTailBranch() {
		TestModelEngine engine = new TestModelEngine(EnumSet.of(ModelModalityEnum.TEXT));
		InputMessage root = newMessage();
		root.addPart(new TextMessagePart("root"));
		InputMessage imageBranch = newMessage();
		imageBranch.setParentMessageId(root.getMessageId());
		imageBranch.addPart(new MediaMessagePart(MessageInputMedia.fromUrl("https://example.com/image.png")));
		InputMessage textBranch = newMessage();
		textBranch.setParentMessageId(root.getMessageId());
		textBranch.addPart(new TextMessagePart("continue here"));

		assertDoesNotThrow(() -> engine.validateInputModalities(List.of(root, imageBranch, textBranch), null));
	}

	private static InputMessage newMessage() {
		Room room = new Room();
		room.setId("test-room");
		return InputMessage.builder(room).build();
	}

	private static MessageInputMedia pdfMedia() {
		return new Gson().fromJson("{\"mimeType\":\"application/pdf\"}", MessageInputMedia.class);
	}

	private static MessageInputMedia audioMedia() {
		return new Gson().fromJson("{\"mimeType\":\"audio/mp3\"}", MessageInputMedia.class);
	}

	private static class TestModelEngine extends AbstractModelEngine {
		private InputMessage lastInputMessage;
		private Map<String, Object> lastHyperParameters;

		private TestModelEngine(Set<ModelModalityEnum> inputModalities) {
			this.inputModalities = inputModalities;
			setEngineName("test-model");
		}

		@Override
		protected AskModelEngineResponse askCall(InputMessage inputMessage, Insight insight, String roomId,
				Map<String, Object> hyperParameters) {
			this.lastInputMessage = inputMessage;
			this.lastHyperParameters = hyperParameters;
			return null;
		}

		@Override
		protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight,
				Map<String, Object> parameters) {
			return null;
		}

		@Override
		public ModelTypeEnum getModelType() {
			return ModelTypeEnum.OPEN_AI;
		}

		@Override
		public void close() throws IOException {
			// Nothing to close in this test engine.
		}
	}
}
