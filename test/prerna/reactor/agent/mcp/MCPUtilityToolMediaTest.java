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
package prerna.reactor.agent.mcp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;

class MCPUtilityToolMediaTest {

	private static final String IMAGE = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aEioAAAAASUVORK5CYII=";

	@TempDir
	Path roomFolder;

	private Room room() {
		Room room = mock(Room.class);
		when(room.getId()).thenReturn("tool-media-room");
		when(room.getRoomFolderPath()).thenReturn(roomFolder.toString());
		return room;
	}

	private JSONObject modelResponse(String fileName) {
		return new JSONObject().put("response", "Generated image").put("messageId", "model-message")
				.put("parts", new JSONArray().put(new JSONObject().put("type", "MEDIA").put("mediaInfo",
						new JSONObject().put("base64Data", IMAGE).put("fileName", fileName)
								.put("fileLocation", "/model-output/" + fileName).put("mimeType", "image/png")
								.put("fileFormat", "png").put("mediaInputType", "FILE"))));
	}

	private JSONObject pixelResponse(Object output) {
		return new JSONObject().put("insightID", "tool-insight").put("pixelReturn", new JSONArray()
				.put(new JSONObject().put("pixelId", "77").put("output", output)
						.put("operationType", new JSONArray().put("OPERATION")))
				.put(new JSONObject().put("pixelId", "78").put("output", "Unrelated result")));
	}

	private JSONArray blocks(String output) {
		return new JSONObject(output).getJSONArray(MCPUtility.SEMOSS_MULTIMODAL_TOOL_RESPONSE_KEY);
	}

	@ParameterizedTest
	@ValueSource(strings = { "GetInsightAssetsBase64", "GetRoomAssetsBase64", "GetAppAssetsBase64",
			"GetEngineAssetsBase64", "GetUserAssetsBase64", "GetStorageFileAsBase64" })
	void rawBase64FromPixelFileReadersBecomesAnImageAttachment(String reactor) throws Exception {
		Room room = room();
		JSONObject pixel = pixelResponse(IMAGE);
		pixel.getJSONArray("pixelReturn").getJSONObject(0).put("pixelExpression",
				reactor + " ( filePath = [ \"generated.png\" ] ) ;");
		String input = new JSONArray().put(pixel).toString();
		try (MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
			String output = MCPUtility.externalizeToolResultMedia(input, room);
			assertFalse(output.contains(IMAGE));
			assertFalse(output.contains("base64Data"));
			JSONArray blocks = blocks(output);
			JSONArray results = new JSONArray(blocks.getJSONObject(0).getString("text"))
					.getJSONObject(0).getJSONArray("pixelReturn");
			assertEquals("Unrelated result", results.getJSONObject(1).getString("output"));
			assertEquals("77", results.getJSONObject(0).getString("pixelId"));
			assertTrue(results.getJSONObject(0).getString("pixelExpression").startsWith(reactor));
			String fileName = blocks.getJSONObject(2).getJSONArray("image").getString(0);
			assertTrue(fileName.endsWith(".png"));
			assertArrayEquals(Base64.getDecoder().decode(IMAGE), Files.readAllBytes(roomFolder.resolve(fileName)));

			InputMessage message = InputMessage.toolExecution(room, "file-call", "runPixel", output, Map.of(), "success", true);
			String modelOutput = new JSONArray(MessageUtils.toJsonArrayWithImageData(List.of(message)))
					.getJSONObject(0).getJSONArray("parts").getJSONObject(0).getJSONObject("toolResult").getString("output");
			JSONObject image = blocks(modelOutput).getJSONObject(2);
			assertEquals("image", image.getString("type"));
			assertEquals("image/png", image.getString("mimeType"));
			assertEquals(IMAGE, image.getString("data"));
			assertFalse(MessageUtils.toJsonArray(List.of(message)).contains(IMAGE));
			cluster.verify(() -> ClusterUtil.pushRoomAsync("tool-media-room"));
		}
	}

	@Test
	void base64PdfFileResultsBecomeDocumentAttachments() throws Exception {
		Room room = room();
		String pdf = Base64.getEncoder().encodeToString("%PDF-1.7\n%%EOF\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
		JSONObject pixel = pixelResponse(pdf);
		pixel.getJSONArray("pixelReturn").getJSONObject(0).put("pixelExpression", "GetInsightAssetsBase64(filePath=[\"file.pdf\"]);");
		try (MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
			String output = MCPUtility.externalizeToolResultMedia(pixel.toString(), room);
			assertFalse(output.contains(pdf));
			String fileName = blocks(output).getJSONObject(2).getJSONArray("image").getString(0);
			assertTrue(fileName.endsWith(".pdf"));
			InputMessage message = InputMessage.toolExecution(room, "pdf-call", "runPixel", output, Map.of(), "success", true);
			String modelOutput = new JSONArray(MessageUtils.toJsonArrayWithImageData(List.of(message)))
					.getJSONObject(0).getJSONArray("parts").getJSONObject(0).getJSONObject("toolResult").getString("output");
			assertEquals("document", blocks(modelOutput).getJSONObject(2).getString("type"));
			assertEquals("application/pdf", blocks(modelOutput).getJSONObject(2).getString("mimeType"));
			assertEquals(pdf, blocks(modelOutput).getJSONObject(2).getString("data"));
		}
	}

	@Test
	void leavesOrdinaryStringsAndFailedFileReadsUnchanged() {
		JSONObject pixel = pixelResponse(IMAGE);
		JSONObject result = pixel.getJSONArray("pixelReturn").getJSONObject(0);
		for (String expression : List.of("Echo(value=[\"GetInsightAssetsBase64(filePath=...)\"]);", "SomeOtherTool();")) {
			result.put("pixelExpression", expression);
			assertEquals(pixel.toString(), MCPUtility.externalizeToolResultMedia(pixel.toString(), room()));
		}
		result.put("pixelExpression", "GetInsightAssetsBase64(filePath=[\"file.png\"]);");
		result.put("output", "File was not found");
		assertEquals(pixel.toString(), MCPUtility.externalizeToolResultMedia(pixel.toString(), room()));
		result.put("output", "FAIL").put("operationType", new JSONArray().put("ERROR"));
		assertEquals(pixel.toString(), MCPUtility.externalizeToolResultMedia(pixel.toString(), room()));
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void pixelMediaIsSavedAndSentAsAnImageWhileSiblingResultsSurvive(boolean arrayWrapper) throws Exception {
		Room room = room();
		JSONObject pixel = pixelResponse(modelResponse("generated.png"));
		String input = arrayWrapper ? new JSONArray().put(pixel).toString() : pixel.toString();
		String output;
		try (MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
			output = MCPUtility.externalizeToolResultMedia(input, room);
			cluster.verify(() -> ClusterUtil.pushRoomAsync("tool-media-room"));
		}

		assertFalse(output.contains(IMAGE));
		assertFalse(output.contains("base64Data"));
		assertArrayEquals(Base64.getDecoder().decode(IMAGE), Files.readAllBytes(roomFolder.resolve("generated.png")));
		JSONArray blocks = blocks(output);
		String text = blocks.getJSONObject(0).getString("text");
		JSONObject preserved = arrayWrapper ? new JSONArray(text).getJSONObject(0) : new JSONObject(text);
		assertEquals("tool-insight", preserved.getString("insightID"));
		JSONArray results = preserved.getJSONArray("pixelReturn");
		assertEquals("Unrelated result", results.getJSONObject(1).getString("output"));
		assertEquals("77", results.getJSONObject(0).getString("pixelId"));
		assertEquals("OPERATION", results.getJSONObject(0).getJSONArray("operationType").getString(0));
		JSONObject model = results.getJSONObject(0).getJSONObject("output");
		assertEquals("Generated image", model.getString("response"));
		assertEquals("model-message", model.getString("messageId"));
		JSONObject media = model.getJSONArray("parts").getJSONObject(0).getJSONObject("mediaInfo");
		assertEquals("generated.png", media.getString("fileLocation"));
		assertEquals("generated.png", blocks.getJSONObject(2).getJSONArray("image").getString(0));

		InputMessage message = InputMessage.toolExecution(room, "image-call", "runPixel", output, Map.of(), "success", true);
		String modelHistory = MessageUtils.toJsonArrayWithImageData(List.of(message));
		String modelOutput = new JSONArray(modelHistory).getJSONObject(0).getJSONArray("parts")
				.getJSONObject(0).getJSONObject("toolResult").getString("output");
		JSONObject image = blocks(modelOutput).getJSONObject(2);
		assertEquals("image", image.getString("type"));
		assertEquals("image/png", image.getString("mimeType"));
		assertEquals(IMAGE, image.getString("data"));
		assertFalse(MessageUtils.toJsonArray(List.of(message)).contains(IMAGE));
	}

	@Test
	void handlesNestedPixelRunsAndMultipleImagesWithoutOverwritingRoomFiles() throws Exception {
		Files.writeString(roomFolder.resolve("generated.png"), "Existing file");
		JSONObject inner = pixelResponse(modelResponse("generated.png"));
		inner.getJSONArray("pixelReturn").put(new JSONObject().put("output", modelResponse("second.png")));
		String input = new JSONArray().put(pixelResponse(new JSONArray().put(inner))).toString();
		try (MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
			String output = MCPUtility.externalizeToolResultMedia(input, room());
			assertFalse(output.contains(IMAGE));
			JSONArray images = blocks(output).getJSONObject(2).getJSONArray("image");
			assertEquals(2, images.length());
			assertFalse("generated.png".equals(images.getString(0)));
			assertEquals("second.png", images.getString(1));
			assertArrayEquals(Base64.getDecoder().decode(IMAGE), Files.readAllBytes(roomFolder.resolve(images.getString(0))));
			assertEquals("Existing file", Files.readString(roomFolder.resolve("generated.png")));
			assertTrue(blocks(output).getJSONObject(0).getString("text").contains(images.getString(0)));
			cluster.verify(() -> ClusterUtil.pushRoomAsync("tool-media-room"));
		}
	}

	@Test
	void directModelResponsesStillUseTheExistingEnvelope() throws Exception {
		try (MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
			JSONArray blocks = blocks(MCPUtility.externalizeToolResultMedia(modelResponse("direct.png").toString(), room()));
			assertEquals(2, blocks.length());
			assertTrue(blocks.getJSONObject(0).getString("text").startsWith("Generated image\n\nMedia output saved"));
			assertEquals("direct.png", blocks.getJSONObject(1).getJSONArray("image").getString(0));
			assertArrayEquals(Base64.getDecoder().decode(IMAGE), Files.readAllBytes(roomFolder.resolve("direct.png")));
		}
	}

	@Test
	void failedSavesReportTheFailureWithoutSendingBase64AsText() throws Exception {
		Room room = room();
		Path blocked = roomFolder.resolve("not-a-directory");
		Files.writeString(blocked, "Keep this file");
		when(room.getRoomFolderPath()).thenReturn(blocked.toString());
		String input = new JSONArray().put(pixelResponse(modelResponse("generated.png"))).toString();
		try (MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
			String output = MCPUtility.externalizeToolResultMedia(input, room);
			assertFalse(output.contains(IMAGE));
			assertFalse(output.contains("base64Data"));
			JSONArray blocks = blocks(output);
			assertEquals(2, blocks.length());
			assertTrue(blocks.getJSONObject(1).getString("text").contains("1 media output(s) could not be saved"));
			assertTrue(blocks.getJSONObject(0).getString("text").contains("Unrelated result"));
			assertEquals("Keep this file", Files.readString(blocked));
			cluster.verifyNoInteractions();
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "ordinary text", "{\"base64Data\":", "[1, null, \"base64Data\"]",
			"{\"pixelReturn\":[{\"output\":{\"base64Data\":\"unrelated\"}}]}",
			"{\"parts\":[{\"type\":\"TEXT\",\"text\":\"base64Data\"}]}" })
	void leavesResultsWithoutModelMediaUnchanged(String output) {
		assertEquals(output, MCPUtility.externalizeToolResultMedia(output, room()));
	}
}
