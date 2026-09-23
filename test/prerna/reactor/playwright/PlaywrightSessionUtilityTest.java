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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;

import prerna.SemossUnitTest;

class PlaywrightSessionUtilityTest extends SemossUnitTest {

	private Playwright playwright;
	private Browser browser;
	private BrowserContext context;
	private Page page;
	private PlaywrightSession session;
	private Path htmlFile;

	@BeforeEach
	void setUp() throws IOException {
		if (Files.exists(tempDir)) {
			FileUtils.cleanDirectory(tempDir.toFile());
		}

		// Create a simple HTML file for testing
		htmlFile = tempDir.resolve("test.html");
		String html = "<!DOCTYPE html>\n" +
				"<html>\n" +
				"<head><title>Test Page</title></head>\n" +
				"<body>\n" +
				"  <h1>Test Page</h1>\n" +
				"  <input type=\"text\" id=\"username\" placeholder=\"Enter username\" />\n" +
				"  <input type=\"password\" id=\"password\" placeholder=\"Enter password\" />\n" +
				"  <button id=\"submit-btn\">Submit</button>\n" +
				"  <button id=\"cancel-btn\" disabled>Cancel</button>\n" +
				"  <div id=\"result\" style=\"display:none;\">Result</div>\n" +
				"  <a href=\"#\" id=\"test-link\">Test Link</a>\n" +
				"</body>\n" +
				"</html>";
		Files.writeString(htmlFile, html);

		// Setup Playwright
		playwright = Playwright.create();
		browser = playwright.webkit().launch(new com.microsoft.playwright.BrowserType.LaunchOptions().setHeadless(true));
		context = browser.newContext();
		page = context.newPage();

		// Create session
		session = new PlaywrightSession(context, page);
	}

	@AfterEach
	void tearDown() {
		if (page != null) {
			page.close();
		}
		if (context != null) {
			context.close();
		}
		if (browser != null) {
			browser.close();
		}
		if (playwright != null) {
			playwright.close();
		}
	}

	// Helper method to create PlaywrightStep with all parameters
	private PlaywrightStep createStep(int id, PlaywrightStepType type, String url, Selector selector,
			Coords coords, String text, String label, boolean isPassword, boolean storeValue,
			Integer waitAfterMs, Integer deltaY, Boolean pressEnter, String tag) {
		return new PlaywrightStep(
			id, type, url, coords, null, null, text, pressEnter, deltaY, null, waitAfterMs,
			null, null, label, null, isPassword, storeValue, selector, null, true, true, null, tag
		);
	}

	@Test
	void resetPagesForNewViewerPreservesContextAndReplacesTabs() {
		Page originalPage = page;
		Page popupPage = context.newPage();
		session.registerPage(popupPage, "tab-2");

		Page replacementPage = session.resetPagesForNewViewer();
		page = replacementPage;

		assertSame(context, session.getBrowserContext());
		assertSame(replacementPage, session.getPage());
		assertEquals(1, session.getTabPages().size());
		assertTrue(originalPage.isClosed());
		assertTrue(popupPage.isClosed());
		assertFalse(replacementPage.isClosed());
	}

	@Test
	void applyStepNavigatesToUrl() {
		Selector selector = new Selector("css", "body", null);
		PlaywrightStep step = createStep(1, PlaywrightStepType.NAVIGATE,
				"file://" + htmlFile.toAbsolutePath().toString(), selector, null, null, null,
				false, false, null, null, null, null);

		Map<String, Object> result = PlaywrightSessionUtility.applyStep(session, step, "tab-1");

		assertNotNull(result);
		assertTrue(page.url().contains("test.html"));
		assertEquals("Test Page", result.get("tabTitle"));
	}

	@Test
	void applyStepTypesTextIntoInput() {
		page.navigate("file://" + htmlFile.toAbsolutePath().toString());

		Selector selector = new Selector("id", "username", null);
		PlaywrightStep step = createStep(1, PlaywrightStepType.TYPE, null, selector, null,
				"testuser", "Username", false, true, null, null, false, "input");

		Map<String, Object> result = PlaywrightSessionUtility.applyStep(session, step, "tab-1");

		assertNotNull(result);
		assertEquals("testuser", page.locator("#username").inputValue());
	}

	@Test
	void applyStepClicksButton() {
		page.navigate("file://" + htmlFile.toAbsolutePath().toString());

		page.evaluate("() => { " +
				"const btn = document.getElementById('submit-btn'); " +
				"btn.addEventListener('click', () => { " +
				"  document.getElementById('result').style.display = 'block'; " +
				"}); " +
				"}");

		Selector selector = new Selector("id", "submit-btn", null);
		PlaywrightStep step = createStep(1, PlaywrightStepType.CLICK, null, selector, null,
				null, "Submit", false, false, null, null, null, "button");

		Map<String, Object> result = PlaywrightSessionUtility.applyStep(session, step, "tab-1");

		assertNotNull(result);
		// Wait a bit for the click to be processed
		page.waitForTimeout(100);
		assertTrue(page.locator("#result").isVisible());
	}

	@Test
	void applyStepScrollsPage() {
		page.navigate("file://" + htmlFile.toAbsolutePath().toString());

		// Add a tall div to enable scrolling
		page.evaluate("() => { document.body.innerHTML += '<div style=\"height:2000px;\">Tall content</div>'; }");

		PlaywrightStep step = createStep(1, PlaywrightStepType.SCROLL, null, null, null,
				null, null, false, false, null, 500, null, null);

		Map<String, Object> result = PlaywrightSessionUtility.applyStep(session, step, "tab-1");

		assertNotNull(result);
		// Verify page was scrolled (scrollY should be > 0)
		Integer scrollY = (Integer) page.evaluate("window.scrollY");
		assertTrue(scrollY > 0);
	}

	@Test
	void applyStepHoversOverElement() {
		page.navigate("file://" + htmlFile.toAbsolutePath().toString());

		page.evaluate("() => { " +
				"const link = document.getElementById('test-link'); " +
				"link.addEventListener('mouseover', () => { " +
				"  document.getElementById('result').style.display = 'block'; " +
				"}); " +
				"}");

		Selector selector = new Selector("id", "test-link", null);
		PlaywrightStep step = createStep(1, PlaywrightStepType.HOVER, null, selector, null,
				null, "Test Link", false, false, null, null, null, "a");

		Map<String, Object> result = PlaywrightSessionUtility.applyStep(session, step, "tab-1");

		assertNotNull(result);
		// Wait a bit for the hover to be processed
		page.waitForTimeout(100);
		assertTrue(page.locator("#result").isVisible());
	}

	@Test
	void applyStepWaitsForTimeout() {
		page.navigate("file://" + htmlFile.toAbsolutePath().toString());

		PlaywrightStep step = createStep(1, PlaywrightStepType.WAIT, null, null, null,
				null, null, false, false, 500, null, null, null);

		long startTime = System.currentTimeMillis();
		Map<String, Object> result = PlaywrightSessionUtility.applyStep(session, step, "tab-1");
		long elapsed = System.currentTimeMillis() - startTime;

		assertNotNull(result);
		assertTrue((Boolean) result.get("shouldStop"));
		assertTrue(elapsed >= 500);
	}

	@Test
	void applyStepModifiesDomSuccessfully() {
		page.navigate("file://" + htmlFile.toAbsolutePath().toString());

        page.evaluate("() => { " +
				"const btn = document.getElementById('submit-btn'); " +
				"btn.addEventListener('click', () => { " +
				"  const div = document.createElement('div'); " +
				"  div.id = 'new-content'; " +
				"  div.innerHTML = '<h2>Content Added</h2><p>New paragraph</p>'; " +
				"  document.body.appendChild(div); " +
				"}); " +
				"}");

		Selector selector = new Selector("id", "submit-btn", null);
		PlaywrightStep step = createStep(1, PlaywrightStepType.CLICK, null, selector, null,
				null, "Submit", false, false, null, null, null, "button");

		Map<String, Object> result = PlaywrightSessionUtility.applyStep(session, step, "tab-1");

		assertNotNull(result);
		// Verify the step executed successfully and DOM was modified
		assertNotNull(result.get("isPageChanged"));
		page.waitForTimeout(100);
		assertTrue(page.locator("#new-content").count() > 0);
	}

	@Test
	void applyStepHandlesMultipleSelectorStrategies() {
		page.navigate("file://" + htmlFile.toAbsolutePath().toString());

		// Test ID selector
		Selector idSelector = new Selector("id", "username", null);
		PlaywrightStep idStep = createStep(1, PlaywrightStepType.TYPE, null, idSelector, null,
				"user1", "Username", false, true, null, null, false, "input");
		PlaywrightSessionUtility.applyStep(session, idStep, "tab-1");
		assertEquals("user1", page.locator("#username").inputValue());

		// Test placeholder selector
		Selector placeholderSelector = new Selector("placeholder", "Enter password", null);
		PlaywrightStep placeholderStep = createStep(2, PlaywrightStepType.TYPE, null, placeholderSelector, null,
				"pass123", "Password", true, true, null, null, false, "input");
		PlaywrightSessionUtility.applyStep(session, placeholderStep, "tab-1");
		assertEquals("pass123", page.locator("#password").inputValue());
	}

	@Test
	void applyStepReturnsIsNewTabFalseWhenNoNewTab() {
		page.navigate("file://" + htmlFile.toAbsolutePath().toString());

		Selector selector = new Selector("id", "submit-btn", null);
		PlaywrightStep step = createStep(1, PlaywrightStepType.CLICK, null, selector, null,
				null, "Submit", false, false, null, null, null, "button");

		Map<String, Object> result = PlaywrightSessionUtility.applyStep(session, step, "tab-1");

		assertNotNull(result);
		assertFalse((Boolean) result.get("isNewTab"));
	}

	@Test
	void applyStepHandlesContextStepType() {
		page.navigate("file://" + htmlFile.toAbsolutePath().toString());

		PlaywrightStep step = createStep(1, PlaywrightStepType.CONTEXT, null, null, null,
				null, null, false, false, null, null, null, null);

		Map<String, Object> result = PlaywrightSessionUtility.applyStep(session, step, "tab-1");

		assertNotNull(result);
		assertFalse((Boolean) result.get("shouldStop"));
	}
}
