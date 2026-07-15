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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ImageContextReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ImageContextReactor.class);

	public ImageContextReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId", ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String tabId = this.keyValue.get(this.keysToGet[1]);
		String engineId = this.keyValue.get(this.keysToGet[2]);
		Map<String, Object> paramValues = getMap(this.keysToGet[3]);

		String userPrompt = (String) paramValues.get("userPrompt");

		Map<String, Object> result = new HashMap<>();
		try {
			ScreenshotReactor screenshotReactor = new ScreenshotReactor();
			screenshotReactor.setInsight(this.insight);
			screenshotReactor.setNounStore(this.store);

			// add sessionId parameter to the noun store
			GenRowStruct sessionGrs = this.store.makeGenRowStruct(ReactorKeysEnum.SESSION_ID.getKey());
			sessionGrs.add(new NounMetadata(sessionId, PixelDataType.CONST_STRING));
			sessionGrs.add(new NounMetadata(tabId, PixelDataType.CONST_STRING));

			GenRowStruct cropGrs = this.store.makeGenRowStruct("cropParams");
			Map<String, Object> cropParams = new HashMap<>();

			cropParams.put("startX", paramValues.get("startX"));
			cropParams.put("startY", paramValues.get("startY"));
			cropParams.put("endX", paramValues.get("endX"));
			cropParams.put("endY", paramValues.get("endY"));
			cropGrs.add(new NounMetadata(cropParams, PixelDataType.MAP));

			NounMetadata screenshotResult = screenshotReactor.execute();

			ScreenshotResponse croppedImage = (ScreenshotResponse) screenshotResult.getValue();

			String instruction = String
					.format("Given the prompt \"%s\", what information would be useful from this image?", userPrompt);

			IModelEngine modelEngine = Utility.getModel(engineId);

			String insightFolder = this.insight.getInsightFolder();

			String imageName = "playwright_screenshot_" + System.currentTimeMillis() + ".png";
			String modelOutput = PlaywrightUtility.callModel(insightFolder, imageName, croppedImage, modelEngine,
					instruction, this.insight);

			result.put("response", modelOutput);
		} catch (Exception e) {
			classLogger.error("Error getting image context", e);
			result.put("response", "Error: " + e.getMessage());
		}

		return new NounMetadata(result, PixelDataType.MAP);
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