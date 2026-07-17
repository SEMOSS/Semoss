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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.LoadState;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ScreenshotReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ScreenshotReactor.class);

	/**
	 * Default constructor for ScreenshotReactor. Initializes the keys this reactor
	 * expects: sessionId, tabId, and paramValues.
	 */
	public ScreenshotReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId", ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	/**
	 * Executes the reactor to capture a screenshot of the current Playwright page.
	 * The screenshot can be a full page or a cropped portion based on provided
	 * parameters.
	 *
	 * @return A NounMetadata object containing a {@link ScreenshotResponse}
	 *         (converted to a Map) with the Base64 encoded image and its
	 *         dimensions.
	 * @throws IllegalArgumentException If the session or tab is not found.
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String tabId = this.keyValue.get(this.keysToGet[1]);
		// check if crop params are provided
		Map<String, Object> paramValues = getMap(this.keysToGet[2]);

		PlaywrightSession playwrightSesion = this.insight.getUser().getPlaywrightSession(sessionId);

		if (paramValues != null && paramValues.containsKey("startX")) {
			// log the crop params
			classLogger.info("Crop params provided.");
			classLogger.info("Crop params: " + paramValues.toString());
			// cropped screenshot
			int startX = ((Number) paramValues.get("startX")).intValue();
			int startY = ((Number) paramValues.get("startY")).intValue();
			int endX = ((Number) paramValues.get("endX")).intValue();
			int endY = ((Number) paramValues.get("endY")).intValue();

			return new NounMetadata(croppedScreenshot(playwrightSesion, tabId, startX, startY, endX, endY),
					PixelDataType.MAP);
		} else {
			// normal screenshot
			return new NounMetadata(screenshot(playwrightSesion, tabId), PixelDataType.MAP);
		}
	}

	/**
	 * Captures a full screenshot of the visible viewport of the specified tab.
	 *
	 * @param playwrightSession The active {@link PlaywrightSession}.
	 * @param tabId             The ID of the tab to capture the screenshot from.
	 * @return A {@link ScreenshotResponse} containing the Base64 encoded image,
	 *         viewport dimensions, and device pixel ratio.
	 */
	public static ScreenshotResponse screenshot(PlaywrightSession playwrightSession, String tabId) {
		Page page = playwrightSession.getPage(tabId);
		waitForStablePage(page);
		playwrightSession.refreshTrackedUrl(tabId);
		byte[] buf = page.screenshot(new Page.ScreenshotOptions().setFullPage(false));
		String b64 = java.util.Base64.getEncoder().encodeToString(buf);

		int vpW = page.viewportSize().width;
		int vpH = page.viewportSize().height;

		Object raw = page.evaluate("() => Number.isFinite(window.devicePixelRatio) ? window.devicePixelRatio : 1");
		double dpr = (raw instanceof Number) ? ((Number) raw).doubleValue() : 1.0;

		return new ScreenshotResponse(b64, vpW, vpH, dpr);
	}

	/**
	 * Captures a cropped screenshot of a specific rectangular area of the specified
	 * tab.
	 *
	 * @param playwrightSession The active {@link PlaywrightSession}.
	 * @param tabId             The ID of the tab to capture the screenshot from.
	 * @param startX            The starting X-coordinate for cropping.
	 * @param startY            The starting Y-coordinate for cropping.
	 * @param endX              The ending X-coordinate for cropping.
	 * @param endY              The ending Y-coordinate for cropping.
	 * @return A {@link ScreenshotResponse} containing the Base64 encoded cropped
	 *         image, its dimensions, and a default DPR of 1.0.
	 */
	public static ScreenshotResponse croppedScreenshot(PlaywrightSession playwrightSession, String tabId, int startX,
			int startY, int endX, int endY) {
		Page page = playwrightSession.getPage(tabId);
		waitForStablePage(page);
		playwrightSession.refreshTrackedUrl(tabId);

		int x = Math.min(startX, endX);
		int y = Math.min(startY, endY);
		int width = Math.abs(endX - startX);
		int height = Math.abs(endY - startY);

		byte[] buf = page.screenshot(new Page.ScreenshotOptions().setFullPage(false).setClip(x, y, width, height));

		String b64 = java.util.Base64.getEncoder().encodeToString(buf);

		return new ScreenshotResponse(b64, width, height, 1.0);
	}

	/**
	 * Waits for the given Playwright page to reach a stable state (network idle,
	 * then load). This method is non-blocking and will attempt to wait for network
	 * idle first, then for the page to load, with timeouts. If both fail, it
	 * proceeds without waiting.
	 *
	 * @param page The Playwright {@link Page} to wait for.
	 */
	private static void waitForStablePage(Page page) {
		try {
			page.waitForLoadState(LoadState.NETWORKIDLE, new Page.WaitForLoadStateOptions().setTimeout(5_000));
		} catch (Exception e) {
			try {
				page.waitForLoadState(LoadState.LOAD, new Page.WaitForLoadStateOptions().setTimeout(2_000));
			} catch (Exception ignored) {
				// give up and fall back to immediate screenshot
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that that return a fresh screenshot for a session";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		} else if (key.equals("tabId")) {
			return "The id of the current tab of the playwright";
		}
		return super.getDescriptionForKey(key);
	}

}
