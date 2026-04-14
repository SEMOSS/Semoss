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
package prerna.reactor.playwright;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.util.AssetUtility;

public class PlaywrightUtility {

	private static final ObjectMapper JSON_MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	/**
	 * The name of the folder where Playwright recordings are stored within a
	 * project's assets.
	 */
	public static final String RECORDINGS_FOLDER_NAME = "recordings";

	/**
	 * Initialize and get the recordings directory
	 *
	 * @param projectId Project ID for context
	 * @return Path to recordings directory
	 */
	public static Path initRecordingsDir(String projectId) {
		try {
			Path dir = Path.of(AssetUtility.getProjectAssetsFolder(projectId), RECORDINGS_FOLDER_NAME);
			Files.createDirectories(dir);
			return dir;
		} catch (Exception ex) {
			throw new RuntimeException("Cannot create recordings directory", ex);
		}
	}

	/**
	 * Load steps from a JSON file
	 * 
	 * @param nameOrPath File name or full path
	 * @return StepsEnvelope containing the loaded steps
	 */
	public static StepsEnvelope loadStepsFromFile(String projectId, String nameOrPath) {
		Path file = resolveRecordingPath(projectId, nameOrPath);
		try {
			return JSON_MAPPER.readValue(file.toFile(), StepsEnvelope.class);
		} catch (Exception e) {
			throw new RuntimeException("Failed to read: " + file, e);
		}
	}

	/**
	 * Resolve a recording file path from name or full path
	 * 
	 * @param nameOrPath File name or full path
	 * @return Resolved Path
	 */
	public static Path resolveRecordingPath(String projectId, String nameOrPath) {
		if (nameOrPath.contains(java.nio.file.FileSystems.getDefault().getSeparator())) {
			return Paths.get(nameOrPath);
		} else {
			String fileName = nameOrPath.endsWith(".json") ? nameOrPath : nameOrPath + ".json";
			return initRecordingsDir(projectId).resolve(fileName);
		}
	}

	/**
	 * Generate timestamp string for file naming
	 * 
	 * @return Formatted timestamp string
	 */
	public static String generateTimestamp() {
		return DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(LocalDateTime.now());
	}

	/**
	 * Sanitize filename by replacing invalid characters
	 * 
	 * @param name Original filename
	 * @return Sanitized filename
	 */
	public static String sanitizeFilename(String name) {
		return name.replaceAll("[^a-zA-Z0-9._-]", "_");
	}

	/**
	 * Calls a vision-enabled model with a cropped image and an instruction. This
	 * overload generates a random roomId.
	 *
	 * @param insightFolder The insight folder path.
	 * @param imageName     The name to save the image as.
	 * @param croppedImage  The {@link ScreenshotResponse} containing the base64
	 *                      encoded cropped image.
	 * @param modelEngine   The {@link IModelEngine} to use for the call.
	 * @param instruction   The instruction/prompt for the model.
	 * @param insight       The current {@link Insight} object.
	 * @return The response content from the model.
	 * @throws IOException If an I/O error occurs during image saving.
	 */
	public static String callModel(String insightFolder, String imageName, ScreenshotResponse croppedImage,
			IModelEngine modelEngine, String instruction, Insight insight) throws IOException {
		return callModel(insightFolder, imageName, croppedImage, modelEngine, instruction, insight,
				UUID.randomUUID().toString());
	}

	/**
	 * Calls a vision-enabled model with a cropped image and an instruction, within
	 * a specified room.
	 *
	 * @param insightFolder The insight folder path.
	 * @param imageName     The name to save the image as.
	 * @param croppedImage  The {@link ScreenshotResponse} containing the base64
	 *                      encoded cropped image.
	 * @param modelEngine   The {@link IModelEngine} to use for the call.
	 * @param instruction   The instruction/prompt for the model.
	 * @param insight       The current {@link Insight} object.
	 * @param roomId        The ID of the room for the conversation history.
	 * @return The response content from the model.
	 * @throws IOException If an I/O error occurs during image saving.
	 */
	public static String callModel(String insightFolder, String imageName, ScreenshotResponse croppedImage,
			IModelEngine modelEngine, String instruction, Insight insight, String roomId) throws IOException {
		String tempImagePath = Paths.get(insightFolder).resolve(imageName).toString();

		byte[] imageBytes = Base64.getDecoder().decode(croppedImage.base64Png());
		try (FileOutputStream fos = new FileOutputStream(tempImagePath)) {
			fos.write(imageBytes);
		}

		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, null);
		List<String> copiedImages = RoomUtils.copyFilesToRoomFolder(Arrays.asList(imageName), room, insight);

		InputMessage inputMessage = InputMessage.builder(room).withText(instruction)
				.withMediaInput(copiedImages.getFirst(), room).build();

		ResponseMessage response = room.ask(inputMessage, modelEngine);
		return response.getContent();
	}

	/**
	 * Checks if an element identified by {@link ElementProbeResponse} matches a
	 * given {@link Selector}.
	 *
	 * @param stepSelector The {@link Selector} to match against.
	 * @param probe        The {@link ElementProbeResponse} containing information
	 *                     about the element to check.
	 * @return True if the element matches the selector, false otherwise.
	 */
	public static boolean matchesSelector(Selector stepSelector, ElementProbeResponse probe) {
		if (stepSelector == null || probe == null) {
			return false;
		}

		String stepStrategy = stepSelector.strategy();
		String stepValue = stepSelector.value();

		switch (stepStrategy) {
		case "testId":
			// check data-testid or data-test-id attributes
			if (probe.attrs() == null) {
				return false;
			}
			String testId = probe.attrs().get("data-testid");
			if (testId == null) {
				testId = probe.attrs().get("data-test-id");
			}
			return stepValue.equals(testId);

		case "role":
			return stepValue.equals(probe.role());

		case "id":
			// check if probes id attribute matches
			if (probe.attrs() == null) {
				return false;
			}
			String id = probe.attrs().get("id");
			return id != null && !id.contains("::") && stepValue.equals(id);

		case "css":
			// css selector match
			if (probe.selector() == null) {
				return false;
			}

			// exact match
			if (stepValue.equals(probe.selector())) {
				return true;
			}

			// If step selector is "body"
			if ("body".equals(stepValue)) {
				return true;
			}

			// the probe's CSS path should contain or be contained by the step's CSS
			return probe.selector().contains(stepValue) || stepValue.contains(probe.selector());

		case "text":
			return probe.labelText() != null && probe.labelText().contains(stepValue);

		case "placeholder":
			return stepValue.equals(probe.placeholder());

		case "xpath":
			return matchesXpathAttributes(stepValue, probe);

		default:
			return false;
		}
	}

	/**
	 * Helper method to check if an element's attributes match conditions specified
	 * in an XPath string. This method only checks for attribute-based conditions
	 * within the XPath (e.g., {@code [@id='value']}).
	 *
	 * @param xpath The XPath string containing attribute conditions.
	 * @param probe The {@link ElementProbeResponse} containing the element's
	 *              attributes.
	 * @return True if all attribute conditions in the XPath match the element's
	 *         attributes, false otherwise.
	 */
	private static boolean matchesXpathAttributes(String xpath, ElementProbeResponse probe) {
		if (probe.attrs() == null) {
			return false;
		}

		// Extract attribute conditions from XPath like [@id='value'] or
		// [@class='value']
		Pattern pattern = Pattern.compile("\\[@([^=]+)=['\"]([^'\"]+)['\"]\\]");
		Matcher matcher = pattern.matcher(xpath);

		boolean foundAttribute = false;
		while (matcher.find()) {
			foundAttribute = true;
			String attrName = matcher.group(1).trim();
			String attrValue = matcher.group(2).trim();

			String actualValue = probe.attrs().get(attrName);
			if (actualValue == null || !actualValue.equals(attrValue)) {
				return false;
			}
		}
		return foundAttribute;
	}
}