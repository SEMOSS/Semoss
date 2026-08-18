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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import prerna.engine.api.ModelModalityEnum;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MediaMessagePart;
import prerna.engine.impl.model.message.MessageInputMedia;
import prerna.engine.impl.model.message.TextMessagePart;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;

class AbstractModelEngineInputModalityUnitTests {

	@Test
	void rejectsImageWhenModelOnlyAllowsTextInput() {
		TestModelEngine engine = new TestModelEngine(Set.of(ModelModalityEnum.TEXT.name()));
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
		TestModelEngine engine = new TestModelEngine(
				Set.of(ModelModalityEnum.TEXT.name(), ModelModalityEnum.IMAGE.name()));
		InputMessage message = newMessage();
		message.addPart(new TextMessagePart("describe this"));
		message.addPart(new MediaMessagePart(MessageInputMedia.fromUrl("https://example.com/image.png")));

		assertDoesNotThrow(() -> engine.validateInputModalities(List.of(), message));
	}

	@Test
	void rejectsPdfWhenModelDoesNotAllowPdfInput() {
		TestModelEngine engine = new TestModelEngine(
				Set.of(ModelModalityEnum.TEXT.name(), ModelModalityEnum.IMAGE.name()));
		InputMessage message = newMessage();
		message.addPart(new MediaMessagePart(pdfMedia()));

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> engine.validateInputModalities(List.of(), message));

		assertTrue(exception.getMessage().contains("does not allow PDF input"));
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
		TestModelEngine engine = new TestModelEngine(Set.of(ModelModalityEnum.TEXT.name()));
		InputMessage root = newMessage();
		root.addPart(new TextMessagePart("root"));
		InputMessage imageBranch = newMessage();
		imageBranch.setParentMessageId(root.getMessageId());
		imageBranch.addPart(new MediaMessagePart(MessageInputMedia.fromUrl("https://example.com/image.png")));
		InputMessage textBranch = newMessage();
		textBranch.setParentMessageId(root.getMessageId());
		textBranch.addPart(new TextMessagePart("continue here"));

		assertDoesNotThrow(
				() -> engine.validateInputModalities(List.of(root, imageBranch), textBranch));
	}

	private static InputMessage newMessage() {
		Room room = new Room();
		room.setId("test-room");
		return InputMessage.builder(room).build();
	}

	private static MessageInputMedia pdfMedia() {
		return new Gson().fromJson("{\"mimeType\":\"application/pdf\"}", MessageInputMedia.class);
	}

	private static class TestModelEngine extends AbstractModelEngine {

		private TestModelEngine(Set<String> inputModalities) {
			this.inputModalities = inputModalities;
			setEngineName("test-model");
		}

		@Override
		protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight,
				String roomId, Map<String, Object> hyperParameters) {
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
