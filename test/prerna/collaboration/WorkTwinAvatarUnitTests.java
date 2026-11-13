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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.imageio.ImageIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MediaMessagePart;
import prerna.engine.impl.model.message.MessageInputMedia;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.TextMessagePart;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * One Digital Twin avatar option: what the owner's photo or description becomes on the way to the image model, what
 * comes back to the browser, and that nothing about the call is saved. No database and no real model.
 */
class WorkTwinAvatarUnitTests {

	private final User user = mock(User.class);
	private final IModelEngine model = mock(IModelEngine.class);
	// the Graph paths the photo was read from
	private final List<String> paths = new ArrayList<>();
	private byte[] photo;
	private RuntimeException photoFailure;
	private final BrainMailHeaderSource graph = new BrainMailHeaderSource() {
		@Override
		public Map<String, Object> me(User user) {
			return Map.of();
		}

		@Override
		public Map<String, Object> manager(User user) {
			return null;
		}

		@Override
		public List<Map<String, Object>> list(User user, String folder, Instant since, int max) {
			return List.of();
		}

		@Override
		public byte[] photo(User user, String path) {
			paths.add(path);
			if (photoFailure != null) {
				throw photoFailure;
			}
			return photo;
		}
	};

	@BeforeEach
	void setUp() {
		photo = null;
		photoFailure = null;
	}

	// a container gets its RDF_Map from its image, so one instance is set apart through its environment
	@Test
	void aSettingIsReadFromRdfMapFirstThenFromTheEnvironment() {
		// PATH is in the environment wherever a JVM runs
		String fromEnvironment = System.getenv("PATH").trim();
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			assertEquals(fromEnvironment, WorkTwinAvatar.setting("PATH"));
			utility.when(() -> Utility.getDIHelperProperty("PATH")).thenReturn("   ");
			assertEquals(fromEnvironment, WorkTwinAvatar.setting("PATH"));
			utility.when(() -> Utility.getDIHelperProperty("PATH")).thenReturn(" from-rdf-map ");
			assertEquals("from-rdf-map", WorkTwinAvatar.setting("PATH"));
			assertNull(WorkTwinAvatar.setting("NO_SUCH_SETTING_IN_RDF_MAP_OR_THE_ENVIRONMENT"));
		}
	}

	@Test
	void theFeatureIsOffUnlessTurnedOnAndACallIsRefusedWhileItIsOff() {
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(() -> Utility.getDIHelperProperty(Constants.COLLAB_AVATAR_ENGINE_ID)).thenReturn("engine-1");
			for (String off : new String[] { "false", "yes", "0" }) {
				utility.when(() -> Utility.getDIHelperProperty(Constants.COLLAB_AVATAR_GENERATION_ENABLED))
						.thenReturn(off);
				assertFalse(WorkTwinAvatar.available(), off);
				IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
						() -> WorkTwinAvatar.generate(user, "a friendly owl", 0));
				assertTrue(refusal.getMessage().contains("turned off"), refusal.getMessage());
			}
			// hiding the button is not the check: the model is not even looked for
			utility.verify(() -> Utility.getModel(anyString()), never());

			utility.when(() -> Utility.getDIHelperProperty(Constants.COLLAB_AVATAR_GENERATION_ENABLED))
					.thenReturn(" TRUE ");
			assertTrue(WorkTwinAvatar.available());
			// on, and past the switch: this instance just has no such model
			IllegalArgumentException noModel = assertThrows(IllegalArgumentException.class,
					() -> WorkTwinAvatar.generate(user, "a friendly owl", 0));
			assertTrue(noModel.getMessage().contains("could not be loaded"), noModel.getMessage());
		}
	}

	@Test
	void noPhotoAndNoDescriptionAsksForADescriptionWithoutCallingTheModel() {
		assertEquals(Map.of("needsDescription", true), WorkTwinAvatar.generate(user, "  ", 0, graph, model));
		assertEquals(List.of("/me/photo/$value"), paths);
		verifyNoInteractions(model);
	}

	// no Microsoft login and no photo set both arrive as an exception from the read
	@Test
	void aPhotoReadThatFailsCountsAsNoPhoto() {
		photoFailure = new IllegalArgumentException("GET request returned HTTP 404");
		assertEquals(Map.of("needsDescription", true), WorkTwinAvatar.generate(user, null, 0, graph, model));
		verifyNoInteractions(model);
	}

	@Test
	void bytesThatAreNotAPictureCountAsNoPhoto() {
		photo = new byte[] { 1, 2, 3, 4 };
		assertEquals(Map.of("needsDescription", true), WorkTwinAvatar.generate(user, null, 0, graph, model));
		verifyNoInteractions(model);
	}

	@Test
	void aPhotoGoesToTheModelAsOnePngAndTheOptionComesBackSmall() throws IOException {
		photo = picture(648, 648, "jpg");
		answer(media(picture(1024, 1024, "jpg")));

		Map<String, Object> option = WorkTwinAvatar.generate(user, null, 0, graph, model);

		BufferedImage sent = read(Base64.getDecoder().decode((String) option.get("image")));
		assertEquals(512, sent.getWidth());
		assertEquals(512, sent.getHeight());
		assertEquals("png", format(Base64.getDecoder().decode((String) option.get("image"))));

		InputMessage message = sentMessage();
		assertEquals(WorkTwinAvatar.OPENING + WorkTwinAvatar.LOOKS[0] + WorkTwinAvatar.FRAMING
				+ WorkTwinAvatar.FROM_PHOTO, message.getFullInputPrompt());
		List<MessageInputMedia> pictures = pictures(message);
		assertEquals(1, pictures.size());
		assertEquals("image/png", pictures.get(0).getMimeType());
		assertNull(pictures.get(0).getSourceUrl());
		byte[] reference = Base64.getDecoder().decode(pictures.get(0).getBase64Data());
		assertEquals("png", format(reference));
		// a small copy: enough to see hair and glasses in, too coarse to copy a face from
		assertEquals(WorkTwinAvatar.REFERENCE_SIDE, read(reference).getWidth());
		assertEquals(WorkTwinAvatar.REFERENCE_SIDE, read(reference).getHeight());
		// what the Python client reads: a base64 picture with its type, never a link
		String json = MessageUtils.toJsonArrayWithImageData(List.<AbstractMessage>of(message));
		assertTrue(json.contains("\"base64Data\""), json.substring(0, Math.min(400, json.length())));
		assertTrue(json.contains("\"mimeType\":\"image/png\""));
		assertFalse(json.contains("sourceUrl"));
	}

	@Test
	void aDescriptionIsUsedWithoutReadingThePhoto() throws IOException {
		photo = picture(648, 648, "jpg");
		answer(media(picture(1024, 1024, "png")));

		Map<String, Object> option = WorkTwinAvatar.generate(user, "  Short dark hair, round glasses ", 0, graph, model);

		assertNotNull(option.get("image"));
		assertTrue(paths.isEmpty());
		InputMessage message = sentMessage();
		assertEquals(WorkTwinAvatar.OPENING + WorkTwinAvatar.LOOKS[0] + WorkTwinAvatar.FRAMING
				+ WorkTwinAvatar.FROM_DESCRIPTION + "Short dark hair, round glasses", message.getFullInputPrompt());
		assertTrue(pictures(message).isEmpty());
	}

	@Test
	void aDescriptionOverTheLimitIsRefusedBeforeTheModelIsCalled() {
		String tooLong = "a".repeat(WorkTwinAvatar.DESCRIPTION_CHARS + 1);
		assertThrows(IllegalArgumentException.class, () -> WorkTwinAvatar.generate(user, tooLong, 0, graph, model));
		assertNotNull(option("a".repeat(WorkTwinAvatar.DESCRIPTION_CHARS)));
	}

	@Test
	void anAnswerWithNoReadablePictureIsAnError() {
		answer(new TextMessagePart("I cannot draw that"),
				new MediaMessagePart(MessageInputMedia.fromBase64("a.png", "image/png", "not base64 !!")),
				media(new byte[] { 9, 9, 9 }));
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> WorkTwinAvatar.generate(user, "a friendly owl", 0, graph, model));
		assertTrue(error.getMessage().contains("returned no picture"), error.getMessage());
	}

	@Test
	void theFirstReadablePictureIsTheOption() throws IOException {
		answer(media(new byte[] { 9, 9, 9 }), media(picture(300, 200, "png")));
		BufferedImage option = read(
				Base64.getDecoder().decode((String) WorkTwinAvatar.generate(user, "a friendly owl", 0, graph, model)
						.get("image")));
		// a picture already small enough keeps its size
		assertEquals(300, option.getWidth());
		assertEquals(200, option.getHeight());
	}

	// the same words three times come back as the same picture three times
	@Test
	void threeNumbersInARowAreThreeDifferentLooksWhereverTheyStart() {
		int looks = WorkTwinAvatar.LOOKS.length;
		assertTrue(looks >= 3);
		assertEquals(looks, Set.of(WorkTwinAvatar.LOOKS).size());
		for (int start : new int[] { 0, 4, 997, -2 }) {
			Set<String> run = new HashSet<>();
			for (int option = 0; option < 3; option++) {
				run.add(WorkTwinAvatar.prompt(start + option, ""));
			}
			assertEquals(3, run.size(), "a run starting at " + start);
		}
		// counted round the looks there are, below zero too
		assertEquals(WorkTwinAvatar.prompt(1, "an owl"), WorkTwinAvatar.prompt(looks + 1, "an owl"));
		assertTrue(WorkTwinAvatar.prompt(-1, "").contains(WorkTwinAvatar.LOOKS[looks - 1]));
		// a caller that names no look still gets one of them
		String any = WorkTwinAvatar.prompt(null, "");
		assertTrue(List.of(WorkTwinAvatar.LOOKS).stream().anyMatch(any::contains));
	}

	@Test
	void theLookAskedForIsTheOneTheModelIsSent() throws IOException {
		answer(media(picture(64, 64, "png")));
		WorkTwinAvatar.generate(user, "a friendly owl", 4, graph, model);
		String sent = sentMessage().getFullInputPrompt();
		assertTrue(sent.startsWith(WorkTwinAvatar.OPENING + WorkTwinAvatar.LOOKS[4]), sent);
		assertTrue(sent.endsWith(WorkTwinAvatar.FROM_DESCRIPTION + "a friendly owl"), sent);
	}

	// Room.ask would write the photo and the picture into a chat folder and push it to cloud storage
	@Test
	void theCallUsesARoomThatIsNeverSaved() {
		option("a friendly owl");
		ArgumentCaptor<Room> room = ArgumentCaptor.forClass(Room.class);
		@SuppressWarnings("unchecked")
		ArgumentCaptor<Map<String, Object>> params = ArgumentCaptor.forClass(Map.class);
		verify(model).askRoom(any(InputMessage.class), room.capture(), params.capture());
		assertNull(room.getValue().getRoomFolderPath());
		assertEquals(room.getValue().getInsight().getInsightId(), room.getValue().getId());
		assertEquals(user, room.getValue().getInsight().getUser());
		assertEquals(Map.of("size", "1024x1024", "quality", "medium", "output_format", "jpeg", "stream", false),
				params.getValue());
	}

	@Test
	void pngScalesTheLongSideDownAndKeepsTheShape() throws IOException {
		BufferedImage scaled = read(WorkTwinAvatar.png(picture(1000, 500, "jpg"), 512));
		assertEquals(512, scaled.getWidth());
		assertEquals(256, scaled.getHeight());
	}

	@Test
	void pngRefusesWhatIsNotAPictureOrIsTooLargeToDecode() throws IOException {
		assertNull(WorkTwinAvatar.png(null, 512));
		assertNull(WorkTwinAvatar.png(new byte[0], 512));
		assertNull(WorkTwinAvatar.png("<html>not a picture</html>".getBytes(), 512));
		// 30 million pixels at one bit each: cheap to build, and over the 25 million a picture may have
		BufferedImage huge = new BufferedImage(6000, 5000, BufferedImage.TYPE_BYTE_BINARY);
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		ImageIO.write(huge, "png", bytes);
		assertNull(WorkTwinAvatar.png(bytes.toByteArray(), 512));
	}

	private Map<String, Object> option(String description) {
		try {
			answer(media(picture(64, 64, "png")));
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		return WorkTwinAvatar.generate(user, description, 0, graph, model);
	}

	private void answer(MessagePart... parts) {
		AskModelEngineResponse<?> answer = mock(AskModelEngineResponse.class);
		when(answer.getParts()).thenReturn(List.of(parts));
		when(model.askRoom(any(InputMessage.class), any(Room.class), anyMap())).thenReturn(answer);
	}

	private InputMessage sentMessage() {
		ArgumentCaptor<InputMessage> message = ArgumentCaptor.forClass(InputMessage.class);
		verify(model).askRoom(message.capture(), any(Room.class), anyMap());
		return message.getValue();
	}

	private static List<MessageInputMedia> pictures(InputMessage message) {
		List<MessageInputMedia> pictures = new ArrayList<>();
		for (MessagePart part : message.getParts()) {
			if (part instanceof MediaMessagePart media) {
				pictures.add(media.getMediaInfo());
			}
		}
		return pictures;
	}

	private static MediaMessagePart media(byte[] bytes) {
		return new MediaMessagePart(
				MessageInputMedia.fromBase64("genImage.jpeg", "image/jpeg", Base64.getEncoder().encodeToString(bytes)));
	}

	private static byte[] picture(int width, int height, String format) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		assertTrue(ImageIO.write(new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB), format, bytes));
		return bytes.toByteArray();
	}

	private static BufferedImage read(byte[] bytes) throws IOException {
		assertNotNull(bytes);
		return ImageIO.read(new ByteArrayInputStream(bytes));
	}

	private static String format(byte[] bytes) throws IOException {
		try (var input = ImageIO.createImageInputStream(new ByteArrayInputStream(bytes))) {
			return ImageIO.getImageReaders(input).next().getFormatName().toLowerCase();
		}
	}
}
