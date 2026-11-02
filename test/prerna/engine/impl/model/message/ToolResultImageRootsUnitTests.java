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
package prerna.engine.impl.model.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.impl.model.Room;
import prerna.reactor.agent.AgentRunner;
import prerna.util.Utility;

/**
 * Image references in tool results resolve against the room's own folder and,
 * for a subagent sharing its parent's files, the source room's folders.
 */
class ToolResultImageRootsUnitTests {

	private static final String RENDER = "_meta/file_info/report.pdf/report_vision_3fa2.pdf";

	@TempDir
	Path base;

	@Test
	void refInSourceRoomIsInlinedForSharingSubagent() throws IOException {
		Path child = Files.createDirectories(base.resolve("room/child"));
		Path parent = Files.createDirectories(base.resolve("room/parent"));
		byte[] pdf = write(parent.resolve(RENDER), "parent pdf");

		JsonArray blocks = resolve(child, List.of(parent.toString()), RENDER);

		assertEquals(1, blocks.size());
		assertInlined(blocks.get(0).getAsJsonObject(), "document", pdf);
	}

	@Test
	void ownRoomCopyWinsOverSourceRoom() throws IOException {
		Path child = Files.createDirectories(base.resolve("room/child"));
		Path parent = Files.createDirectories(base.resolve("room/parent"));
		byte[] own = write(child.resolve("chart.png"), "own chart");
		write(parent.resolve("chart.png"), "parent chart");

		JsonArray blocks = resolve(child, List.of(parent.toString()), "chart.png");

		assertInlined(blocks.get(0).getAsJsonObject(), "image", own);
	}

	@Test
	void refOutsideEveryRootIsNotRead() throws IOException {
		Path child = Files.createDirectories(base.resolve("room/child"));
		Path parent = Files.createDirectories(base.resolve("room/parent"));
		write(base.resolve("room/other/secret.png"), "other room");

		JsonArray blocks = resolve(child, List.of(parent.toString()), "../other/secret.png");

		assertEquals(1, blocks.size());
		assertEquals("text", blocks.get(0).getAsJsonObject().get("type").getAsString());
		assertTrue(blocks.get(0).getAsJsonObject().get("text").getAsString().contains("could not be attached"));
	}

	@Test
	void missingRefKeepsTheOtherImages() throws IOException {
		Path child = Files.createDirectories(base.resolve("room/child"));
		byte[] good = write(child.resolve("good.png"), "good");

		JsonArray blocks = resolve(child, List.of(), "missing.png", "good.png");

		assertEquals(2, blocks.size());
		assertEquals("text", blocks.get(0).getAsJsonObject().get("type").getAsString());
		assertInlined(blocks.get(1).getAsJsonObject(), "image", good);
	}

	@Test
	void roomWithoutSourceRoomSharesNothing() {
		Room room = new Room();
		room.setId("child");
		assertEquals(List.of(), room.getSharedFileRoots());
	}

	@Test
	void sharingRoomGetsWorkingDirThenSourceRoot() throws IOException {
		Path parent = Files.createDirectories(base.resolve("room/parent"));
		Path workDir = Files.createDirectories(parent.resolve("analysis"));
		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS)) {
			utility.when(Utility::getBaseFolder).thenReturn(base.toString());

			Room room = sharingRoom("parent", workDir.toString());
			assertEquals(List.of(workDir.toRealPath().toString(), parent.toRealPath().toString()),
					room.getSharedFileRoots());

			// the source room's own root as working dir is listed once
			Room atRoot = sharingRoom("parent", parent.toString());
			assertEquals(List.of(parent.toRealPath().toString()), atRoot.getSharedFileRoots());
		}
	}

	@Test
	void workingDirOutsideSourceRoomIsIgnored() throws IOException {
		Path parent = Files.createDirectories(base.resolve("room/parent"));
		Path elsewhere = Files.createDirectories(base.resolve("room/other"));
		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS)) {
			utility.when(Utility::getBaseFolder).thenReturn(base.toString());

			Room room = sharingRoom("parent", elsewhere.toString());
			assertEquals(List.of(parent.toRealPath().toString()), room.getSharedFileRoots());
		}
	}

	@Test
	void sourceRoomIdThatIsNotAFolderNameSharesNothing() {
		for (String id : List.of("../other", "a/b", "..")) {
			Room room = sharingRoom(id, null);
			assertEquals(List.of(), room.getSharedFileRoots(), id);
		}
	}

	private static Room sharingRoom(String sourceRoomId, String workDir) {
		Room room = new Room();
		room.setId("child");
		Map<String, Object> options = new HashMap<>();
		options.put(AgentRunner.ROOM_OPTION_WORKING_DIR_SOURCE_ROOM, sourceRoomId);
		if (workDir != null) {
			options.put(AgentRunner.ROOM_OPTION_WORKING_DIR, workDir);
		}
		room.setOptionsMap(options);
		return room;
	}

	/** Runs one tool result referencing {@code refs} through the provider serializer. */
	private static JsonArray resolve(Path ownFolder, List<String> sharedRoots, String... refs) {
		JsonArray refArray = new JsonArray();
		for (String ref : refs) {
			refArray.add(ref);
		}
		JsonObject imageBlock = new JsonObject();
		imageBlock.addProperty("type", "image");
		imageBlock.add("image", refArray);
		JsonArray envelopeBlocks = new JsonArray();
		envelopeBlocks.add(imageBlock);
		JsonObject envelope = new JsonObject();
		envelope.add("SEMOSSMultimodalToolResponse", envelopeBlocks);

		Room room = new Room();
		room.setId("child");
		InputMessage message = InputMessage.toolExecution(room, "call-1", "ReadRoomDocuments", envelope.toString(),
				null, "success", false);
		message.roomFolderPath = ownFolder.toString();
		message.sharedFileRoots = sharedRoots;

		JsonObject provider = JsonParser.parseString(MessageUtils.toJsonArrayWithImageData(List.of(message)))
				.getAsJsonArray().get(0).getAsJsonObject();
		String output = provider.getAsJsonArray("parts").get(0).getAsJsonObject().getAsJsonObject("toolResult")
				.get("output").getAsString();
		return JsonParser.parseString(output).getAsJsonObject().getAsJsonArray("SEMOSSMultimodalToolResponse");
	}

	// mime detection reads the file's leading bytes before its extension
	private static final byte[] PNG_SIGNATURE = { (byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' };

	private static byte[] write(Path file, String content) throws IOException {
		Files.createDirectories(file.getParent());
		byte[] text = content.getBytes(StandardCharsets.UTF_8);
		byte[] bytes = text;
		if (file.toString().endsWith(".png")) {
			bytes = new byte[PNG_SIGNATURE.length + text.length];
			System.arraycopy(PNG_SIGNATURE, 0, bytes, 0, PNG_SIGNATURE.length);
			System.arraycopy(text, 0, bytes, PNG_SIGNATURE.length, text.length);
		}
		Files.write(file, bytes);
		return bytes;
	}

	private static void assertInlined(JsonObject block, String type, byte[] expected) {
		assertEquals(type, block.get("type").getAsString());
		assertEquals(Base64.getEncoder().encodeToString(expected), block.get("data").getAsString());
	}
}
