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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteStepReactorTest {

	private DeleteStepReactor reactor;

	@Mock
	private Insight mockInsight;

	@Mock
	private User mockUser;

	@Mock
	private PlaywrightSession mockSession;

	@Mock
	private NounStore mockNounStore;

	@BeforeEach
	public void setUp() {
		MockitoAnnotations.openMocks(this);
		reactor = new DeleteStepReactor();
		reactor.setInsight(mockInsight);
		reactor.setNounStore(mockNounStore);
		// Initialize keyValue map for test execution
		reactor.keyValue = new java.util.HashMap<>();

		when(mockInsight.getUser()).thenReturn(mockUser);
		when(mockNounStore.size()).thenReturn(0);
	}

	@Test
	public void testExecute_SuccessfulDeletion() {
		String sessionId = "session123";
		String tabId = "tab-1";
		int stepId = 2;

		reactor.keyValue.put("sessionId", sessionId);
		reactor.keyValue.put("tabId", tabId);
		reactor.keyValue.put("stepId", String.valueOf(stepId));

		when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

		PlaywrightStep step1 = mock(PlaywrightStep.class);
		when(step1.id()).thenReturn(1);

		PlaywrightStep step2 = mock(PlaywrightStep.class);
		when(step2.id()).thenReturn(stepId);

		PlaywrightStep step3 = mock(PlaywrightStep.class);
		when(step3.id()).thenReturn(3);

		List<PlaywrightStep> pageSteps = new ArrayList<>();
		pageSteps.add(step1);
		pageSteps.add(step2);
		pageSteps.add(step3);

		List<List<PlaywrightStep>> pages = new ArrayList<>();
		pages.add(pageSteps);

		Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
		stepsMap.put(tabId, pages);

		StepsEnvelope history = mock(StepsEnvelope.class);
		when(history.steps()).thenReturn(stepsMap);
		mockSession.history = history;

		try (MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {
			ScreenshotResponse mockScreenshotResponse = mock(ScreenshotResponse.class);
			mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
					.thenReturn(mockScreenshotResponse);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.MAP, result.getNounType());

			@SuppressWarnings("unchecked")
			Map<String, Object> response = (Map<String, Object>) result.getValue();
			assertEquals(true, response.get("success"));
			assertEquals(stepId, response.get("deletedStepId"));
			assertNotNull(response.get("screenshot"));
			assertTrue(response.get("message").toString().contains("Step deleted"));

			assertEquals(2, pageSteps.size());
			assertTrue(pageSteps.stream().noneMatch(s -> s.id() == stepId));
		}
	}

	@Test
	public void testExecute_SessionNotFound_ThrowsException() {
		String sessionId = "nonexistent";
		reactor.keyValue.put("sessionId", sessionId);
		reactor.keyValue.put("tabId", "tab-1");
		reactor.keyValue.put("stepId", "1");

		when(mockUser.getPlaywrightSession(sessionId)).thenReturn(null);

		assertThrows(IllegalStateException.class, () -> reactor.execute());
	}

	@Test
	public void testExecute_StepNotFound_ThrowsException() {
		String sessionId = "session123";
		String tabId = "tab-1";
		int stepId = 999;

		reactor.keyValue.put("sessionId", sessionId);
		reactor.keyValue.put("tabId", tabId);
		reactor.keyValue.put("stepId", String.valueOf(stepId));

		when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

		PlaywrightStep step1 = mock(PlaywrightStep.class);
		when(step1.id()).thenReturn(1);

		List<PlaywrightStep> pageSteps = new ArrayList<>();
		pageSteps.add(step1);

		List<List<PlaywrightStep>> pages = new ArrayList<>();
		pages.add(pageSteps);

		Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
		stepsMap.put(tabId, pages);

		StepsEnvelope history = mock(StepsEnvelope.class);
		when(history.steps()).thenReturn(stepsMap);
		mockSession.history = history;

		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	public void testExecute_TabNotFound_ThrowsException() {
		String sessionId = "session123";
		String tabId = "nonexistent-tab";

		reactor.keyValue.put("sessionId", sessionId);
		reactor.keyValue.put("tabId", tabId);
		reactor.keyValue.put("stepId", "1");

		when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

		Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();

		StepsEnvelope history = mock(StepsEnvelope.class);
		when(history.steps()).thenReturn(stepsMap);
		mockSession.history = history;

		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	public void testExecute_MissingSessionId_ThrowsException() {
		reactor.keyValue.put("tabId", "tab-1");
		reactor.keyValue.put("stepId", "1");

		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	public void testExecute_MissingTabId_ThrowsException() {
		reactor.keyValue.put("sessionId", "session123");
		reactor.keyValue.put("stepId", "1");

		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	public void testExecute_MissingStepId_ThrowsException() {
		reactor.keyValue.put("sessionId", "session123");
		reactor.keyValue.put("tabId", "tab-1");

		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	public void testGetReactorDescription() {
		String description = reactor.getReactorDescription();
		assertNotNull(description);
		assertFalse(description.isEmpty());
		assertTrue(description.contains("Delete") || description.contains("delete") || description.contains("step"));
	}

	@Test
	public void testGetDescriptionForSessionIdKey() {
		String description = reactor.getDescriptionForKey("sessionId");
		assertNotNull(description);
		assertFalse(description.isEmpty());
		assertTrue(description.contains("session") || description.contains("Session"));
	}

	@Test
	public void testGetDescriptionForTabIdKey() {
		String description = reactor.getDescriptionForKey("tabId");
		assertNotNull(description);
		assertFalse(description.isEmpty());
		assertTrue(description.contains("tab") || description.contains("Tab"));
	}

	@Test
	public void testGetDescriptionForStepIdKey() {
		String description = reactor.getDescriptionForKey("stepId");
		assertNotNull(description);
		assertFalse(description.isEmpty());
		assertTrue(description.contains("step") || description.contains("Step"));
	}

	@Test
	public void testKeysToGet() {
		String[] expectedKeys = { "sessionId", "tabId", "stepId" };
		assertArrayEquals(expectedKeys, reactor.keysToGet);
	}

	@Test
	public void testExecute_DeleteMultipleSteps() {
		String sessionId = "session123";
		String tabId = "tab-1";

		when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

		PlaywrightStep step1 = mock(PlaywrightStep.class);
		when(step1.id()).thenReturn(1);

		PlaywrightStep step2 = mock(PlaywrightStep.class);
		when(step2.id()).thenReturn(2);

		PlaywrightStep step3 = mock(PlaywrightStep.class);
		when(step3.id()).thenReturn(3);

		List<PlaywrightStep> pageSteps = new ArrayList<>();
		pageSteps.add(step1);
		pageSteps.add(step2);
		pageSteps.add(step3);

		List<List<PlaywrightStep>> pages = new ArrayList<>();
		pages.add(pageSteps);

		Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
		stepsMap.put(tabId, pages);

		StepsEnvelope history = mock(StepsEnvelope.class);
		when(history.steps()).thenReturn(stepsMap);
		mockSession.history = history;

		try (MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {
			ScreenshotResponse mockScreenshotResponse = mock(ScreenshotResponse.class);
			mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
					.thenReturn(mockScreenshotResponse);

			reactor.keyValue.put("sessionId", sessionId);
			reactor.keyValue.put("tabId", tabId);
			reactor.keyValue.put("stepId", "1");

			NounMetadata result1 = reactor.execute();
			assertNotNull(result1);
			assertEquals(2, pageSteps.size());

			reactor.keyValue.clear();
			reactor.keyValue.put("sessionId", sessionId);
			reactor.keyValue.put("tabId", tabId);
			reactor.keyValue.put("stepId", "3");

			NounMetadata result2 = reactor.execute();
			assertNotNull(result2);
			assertEquals(1, pageSteps.size());

			assertEquals(2, pageSteps.get(0).id());
		}
	}

	@Test
	public void testExecute_InvalidStepIdFormat_ThrowsException() {
		reactor.keyValue.put("sessionId", "session123");
		reactor.keyValue.put("tabId", "tab-1");
		reactor.keyValue.put("stepId", "not-a-number");

		assertThrows(NumberFormatException.class, () -> reactor.execute());
	}

	@Test
	public void testExecute_EmptyPages() {
		String sessionId = "session123";
		String tabId = "tab-1";
		int stepId = 1;

		reactor.keyValue.put("sessionId", sessionId);
		reactor.keyValue.put("tabId", tabId);
		reactor.keyValue.put("stepId", String.valueOf(stepId));

		when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

		List<List<PlaywrightStep>> pages = new ArrayList<>();
		pages.add(new ArrayList<>());

		Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
		stepsMap.put(tabId, pages);

		StepsEnvelope history = mock(StepsEnvelope.class);
		when(history.steps()).thenReturn(stepsMap);
		mockSession.history = history;

		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	public void testExecute_MultiplePages() {
		String sessionId = "session123";
		String tabId = "tab-1";
		int stepId = 3;

		reactor.keyValue.put("sessionId", sessionId);
		reactor.keyValue.put("tabId", tabId);
		reactor.keyValue.put("stepId", String.valueOf(stepId));

		when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

		PlaywrightStep step1 = mock(PlaywrightStep.class);
		when(step1.id()).thenReturn(1);

		PlaywrightStep step2 = mock(PlaywrightStep.class);
		when(step2.id()).thenReturn(2);

		PlaywrightStep step3 = mock(PlaywrightStep.class);
		when(step3.id()).thenReturn(stepId);

		List<PlaywrightStep> page1 = new ArrayList<>();
		page1.add(step1);

		List<PlaywrightStep> page2 = new ArrayList<>();
		page2.add(step2);
		page2.add(step3);

		List<List<PlaywrightStep>> pages = new ArrayList<>();
		pages.add(page1);
		pages.add(page2);

		Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
		stepsMap.put(tabId, pages);

		StepsEnvelope history = mock(StepsEnvelope.class);
		when(history.steps()).thenReturn(stepsMap);
		mockSession.history = history;

		try (MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {
			ScreenshotResponse mockScreenshotResponse = mock(ScreenshotResponse.class);
			mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
					.thenReturn(mockScreenshotResponse);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.MAP, result.getNounType());

			@SuppressWarnings("unchecked")
			Map<String, Object> response = (Map<String, Object>) result.getValue();
			assertEquals(true, response.get("success"));
			assertEquals(stepId, response.get("deletedStepId"));

			assertEquals(1, page2.size());
			assertEquals(2, page2.get(0).id());
		}
	}

	@Test
	public void testExecute_LastStepInPage() {
		String sessionId = "session123";
		String tabId = "tab-1";
		int stepId = 1;

		reactor.keyValue.put("sessionId", sessionId);
		reactor.keyValue.put("tabId", tabId);
		reactor.keyValue.put("stepId", String.valueOf(stepId));

		when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

		PlaywrightStep step1 = mock(PlaywrightStep.class);
		when(step1.id()).thenReturn(stepId);

		List<PlaywrightStep> pageSteps = new ArrayList<>();
		pageSteps.add(step1);

		List<List<PlaywrightStep>> pages = new ArrayList<>();
		pages.add(pageSteps);

		Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
		stepsMap.put(tabId, pages);

		StepsEnvelope history = mock(StepsEnvelope.class);
		when(history.steps()).thenReturn(stepsMap);
		mockSession.history = history;

		try (MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {
			ScreenshotResponse mockScreenshotResponse = mock(ScreenshotResponse.class);
			mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
					.thenReturn(mockScreenshotResponse);

			NounMetadata result = reactor.execute();

			@SuppressWarnings("unchecked")
			Map<String, Object> response = (Map<String, Object>) result.getValue();
			assertEquals(true, response.get("success"));

			assertTrue(pageSteps.isEmpty());
		}
	}

	@Test
	public void testExecute_NegativeStepId() {
		reactor.keyValue.put("sessionId", "session123");
		reactor.keyValue.put("tabId", "tab-1");
		reactor.keyValue.put("stepId", "-1");

		when(mockUser.getPlaywrightSession("session123")).thenReturn(mockSession);

		PlaywrightStep step1 = mock(PlaywrightStep.class);
		when(step1.id()).thenReturn(1);

		List<PlaywrightStep> pageSteps = new ArrayList<>();
		pageSteps.add(step1);

		List<List<PlaywrightStep>> pages = new ArrayList<>();
		pages.add(pageSteps);

		Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
		stepsMap.put("tab-1", pages);

		StepsEnvelope history = mock(StepsEnvelope.class);
		when(history.steps()).thenReturn(stepsMap);
		mockSession.history = history;

		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	public void testExecute_ZeroStepId() {
		reactor.keyValue.put("sessionId", "session123");
		reactor.keyValue.put("tabId", "tab-1");
		reactor.keyValue.put("stepId", "0");

		when(mockUser.getPlaywrightSession("session123")).thenReturn(mockSession);

		PlaywrightStep step1 = mock(PlaywrightStep.class);
		when(step1.id()).thenReturn(1);

		List<PlaywrightStep> pageSteps = new ArrayList<>();
		pageSteps.add(step1);

		List<List<PlaywrightStep>> pages = new ArrayList<>();
		pages.add(pageSteps);

		Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
		stepsMap.put("tab-1", pages);

		StepsEnvelope history = mock(StepsEnvelope.class);
		when(history.steps()).thenReturn(stepsMap);
		mockSession.history = history;

		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}


	@Test
	public void testExecute_ScreenshotReturnsNull() {
		String sessionId = "session123";
		String tabId = "tab-1";
		int stepId = 1;

		reactor.keyValue.put("sessionId", sessionId);
		reactor.keyValue.put("tabId", tabId);
		reactor.keyValue.put("stepId", String.valueOf(stepId));

		when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

		PlaywrightStep step1 = mock(PlaywrightStep.class);
		when(step1.id()).thenReturn(stepId);

		List<PlaywrightStep> pageSteps = new ArrayList<>();
		pageSteps.add(step1);

		List<List<PlaywrightStep>> pages = new ArrayList<>();
		pages.add(pageSteps);

		Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
		stepsMap.put(tabId, pages);

		StepsEnvelope history = mock(StepsEnvelope.class);
		when(history.steps()).thenReturn(stepsMap);
		mockSession.history = history;

		try (MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {
			mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
					.thenReturn(null);

			NounMetadata result = reactor.execute();

			@SuppressWarnings("unchecked")
			Map<String, Object> response = (Map<String, Object>) result.getValue();
			assertEquals(true, response.get("success"));
			assertNull(response.get("screenshot"));
		}
	}

	@Test
	public void testReactorKeyConfiguration() {
		assertNotNull(reactor.keysToGet);
		assertEquals(3, reactor.keysToGet.length);
		assertEquals("sessionId", reactor.keysToGet[0]);
		assertEquals("tabId", reactor.keysToGet[1]);
		assertEquals("stepId", reactor.keysToGet[2]);
	}
}
