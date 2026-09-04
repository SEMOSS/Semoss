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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ImageContextReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ImageContextReactor.class);
	private static final int MAX_PROMPT_LENGTH = 4_000;

	public ImageContextReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId", ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		Map<String, Object> result = new LinkedHashMap<>();
		try {
			String sessionId = requireText(this.keyValue.get(this.keysToGet[0]), "sessionId");
			String tabId = requireText(this.keyValue.get(this.keysToGet[1]), "tabId");
			String engineId = requireText(this.keyValue.get(this.keysToGet[2]), "engine");
			Map<String, Object> paramValues = getMap(this.keysToGet[3]);
			String userPrompt = requireText(paramValues.get("userPrompt"), "userPrompt");
			if (userPrompt.length() > MAX_PROMPT_LENGTH) {
				throw new IllegalArgumentException("userPrompt exceeds " + MAX_PROMPT_LENGTH + " characters");
			}
			if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException(
						"Model " + engineId + " does not exist or user does not have access");
			}

			int startX = requireCoordinate(paramValues, "startX");
			int startY = requireCoordinate(paramValues, "startY");
			int endX = requireCoordinate(paramValues, "endX");
			int endY = requireCoordinate(paramValues, "endY");
			if (startX == endX || startY == endY) {
				throw new IllegalArgumentException("Image context selection must have a non-zero width and height");
			}

			PlaywrightSession session = this.insight.getUser().getPlaywrightSession(sessionId);
			ScreenshotResponse croppedImage = ScreenshotReactor.croppedScreenshot(session, tabId, startX, startY, endX,
					endY);

			String instruction = String
					.format("Given the prompt \"%s\", what information would be useful from this image?", userPrompt);

			IModelEngine modelEngine = Utility.getModel(engineId);

			String insightFolder = this.insight.getInsightFolder();

			String imageName = "playwright_screenshot_" + System.currentTimeMillis() + ".png";
			String modelOutput = PlaywrightUtility.callModel(insightFolder, imageName, croppedImage, modelEngine,
					instruction, this.insight);

			result.put("success", true);
			result.put("response", modelOutput);
			result.put("engineId", engineId);
		} catch (Exception e) {
			classLogger.warn("Image context generation failed: {}", e.getMessage());
			result.put("success", false);
			result.put("error", e.getMessage() == null ? "Image context generation failed" : e.getMessage());
		}

		return new NounMetadata(result, PixelDataType.MAP);
	}

	private static String requireText(Object value, String name) {
		String text = value == null ? "" : value.toString().trim();
		if (text.isBlank()) {
			throw new IllegalArgumentException(name + " is required");
		}
		return text;
	}

	private static int requireCoordinate(Map<String, Object> values, String name) {
		Object value = values.get(name);
		if (!(value instanceof Number)) {
			throw new IllegalArgumentException(name + " must be a number");
		}
		int coordinate = ((Number) value).intValue();
		if (coordinate < 0) {
			throw new IllegalArgumentException(name + " must not be negative");
		}
		return coordinate;
	}

	@Override
	public String getReactorDescription() {
		return "Analyzes a cropped image (screenshot) of a webpage using a vision-enabled LLM to extract relevant context based on a user's prompt.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		} else if (key.equals("engineId")) {
			return "The id of the Model Engine";
		} else if (key.equals("userPrompt")) {
			return "The custom prompt from the user for the LLM";
		} else if (key.equals("startX")) {
			return "The start x coordinates for the clipped image";
		} else if (key.equals("startY")) {
			return "The start y coordinates for the clipped image";
		} else if (key.equals("endX")) {
			return "The end x coordinates for the clipped image";
		} else if (key.equals("endY")) {
			return "The end y coordinates for the clipped image";
		}

		return super.getDescriptionForKey(key);
	}
}
