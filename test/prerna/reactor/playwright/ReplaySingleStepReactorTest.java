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

import java.util.*;

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


public class ReplaySingleStepReactorTest {

    private ReplaySingleStepReactor reactor;

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
        reactor = new ReplaySingleStepReactor();
        reactor.setInsight(mockInsight);
        reactor.setNounStore(mockNounStore);
        // Initialize keyValue map for test execution
        reactor.keyValue = new java.util.HashMap<>();

        when(mockInsight.getUser()).thenReturn(mockUser);
        when(mockNounStore.size()).thenReturn(0);
    }

    @Test
    public void testExecute_Success() {
        reactor.keyValue.put("project", "project123");
        reactor.keyValue.put("sessionId", "session123");
        reactor.keyValue.put("fileName", "recording.json");
        reactor.keyValue.put("stepId", "1");
        reactor.keyValue.put("tabId", "tab-1");

        when(mockUser.getPlaywrightSession("session123")).thenReturn(mockSession);

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class);
             MockedStatic<PlaywrightSessionUtility> mockedSessionUtility = mockStatic(PlaywrightSessionUtility.class);
             MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {

            StepsEnvelope mockEnvelope = mock(StepsEnvelope.class);
            Map<String, List<List<PlaywrightStep>>> stepsMap = createMockStepsMap(1);

            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile("project123", "recording.json"))
                    .thenReturn(mockEnvelope);
            when(mockEnvelope.steps()).thenReturn(stepsMap);

            Map<String, Object> executionResult = new HashMap<>();
            executionResult.put("shouldStop", false);
            executionResult.put("isNewTab", false);

            mockedSessionUtility.when(() -> PlaywrightSessionUtility.applyStep(any(), any(), anyString()))
                    .thenReturn(executionResult);

            ScreenshotResponse mockScreenshotObj = mock(ScreenshotResponse.class);
            mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, "tab-1"))
                    .thenReturn(mockScreenshotObj);

            NounMetadata result = reactor.execute();

            assertNotNull(result);
            assertEquals(PixelDataType.MAP, result.getNounType());
        }
    }

    @Test
    public void testReplayStep_StepNotFound() {
        String projectId = "project123";
        String sessionId = "session123";
        String fileName = "recording.json";
        int stepId = 999;
        String tabId = "tab-1";

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class)) {
            StepsEnvelope mockEnvelope = mock(StepsEnvelope.class);
            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile(projectId, fileName))
                    .thenReturn(mockEnvelope);
            when(mockEnvelope.steps()).thenReturn(new HashMap<>());

            Map<String, Object> result = reactor.replayStep(projectId, sessionId, fileName, stepId, null, tabId);

            assertEquals("failed", result.get("status"));
            assertTrue(result.get("error").toString().contains("Step with ID 999 not found"));
        }
    }

    @Test
    public void testReplayStep_SessionNotFound() {
        String projectId = "project123";
        String sessionId = "session123";
        String fileName = "recording.json";
        int stepId = 1;
        String tabId = "tab-1";

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class)) {
            StepsEnvelope mockEnvelope = mock(StepsEnvelope.class);
            Map<String, List<List<PlaywrightStep>>> stepsMap = createMockStepsMap(1);

            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile(projectId, fileName))
                    .thenReturn(mockEnvelope);
            when(mockEnvelope.steps()).thenReturn(stepsMap);
            when(mockUser.getPlaywrightSession(sessionId)).thenReturn(null);

            Map<String, Object> result = reactor.replayStep(projectId, sessionId, fileName, stepId, null, tabId);

            assertEquals("failed", result.get("status"));
            assertEquals("Session not found", result.get("error"));
        }
    }

    @Test
    public void testReplayStep_SuccessfulExecution() {
        String projectId = "project123";
        String sessionId = "session123";
        String fileName = "recording.json";
        int stepId = 1;
        String tabId = "tab-1";

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class);
             MockedStatic<PlaywrightSessionUtility> mockedSessionUtility = mockStatic(PlaywrightSessionUtility.class);
             MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {

            StepsEnvelope mockEnvelope = mock(StepsEnvelope.class);
            Map<String, List<List<PlaywrightStep>>> stepsMap = createMockStepsMap(1);

            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile(projectId, fileName))
                    .thenReturn(mockEnvelope);
            when(mockEnvelope.steps()).thenReturn(stepsMap);
            when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

            Map<String, Object> executionResult = new HashMap<>();
            executionResult.put("shouldStop", false);
            executionResult.put("isNewTab", false);

            mockedSessionUtility.when(() -> PlaywrightSessionUtility.applyStep(any(), any(), anyString()))
                    .thenReturn(executionResult);

            ScreenshotResponse mockScreenshotObj = mock(ScreenshotResponse.class);
            mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
                    .thenReturn(mockScreenshotObj);

            Map<String, Object> result = reactor.replayStep(projectId, sessionId, fileName, stepId, null, tabId);

            assertEquals("success", result.get("status"));
            assertEquals(1, result.get("stepId"));
            assertEquals(false, result.get("isNewTab"));
            assertNotNull(result.get("screenshot"));
        }
    }

    @Test
    public void testReplayStep_WithNewTabCreation() {
        String projectId = "project123";
        String sessionId = "session123";
        String fileName = "recording.json";
        int stepId = 1;
        String tabId = "tab-1";

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class);
             MockedStatic<PlaywrightSessionUtility> mockedSessionUtility = mockStatic(PlaywrightSessionUtility.class);
             MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {

            StepsEnvelope mockEnvelope = mock(StepsEnvelope.class);
            Map<String, List<List<PlaywrightStep>>> stepsMap = createMockStepsMap(1);

            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile(projectId, fileName))
                    .thenReturn(mockEnvelope);
            when(mockEnvelope.steps()).thenReturn(stepsMap);
            when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

            Map<String, Object> executionResult = new HashMap<>();
            executionResult.put("shouldStop", false);
            executionResult.put("isNewTab", true);
            executionResult.put("newTabId", "tab-2");
            executionResult.put("tabTitle", "New Tab");

            mockedSessionUtility.when(() -> PlaywrightSessionUtility.applyStep(any(), any(), anyString()))
                    .thenReturn(executionResult);

            ScreenshotResponse mockScreenshotObj = mock(ScreenshotResponse.class);
            mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
                    .thenReturn(mockScreenshotObj);

            Map<String, Object> result = reactor.replayStep(projectId, sessionId, fileName, stepId, null, tabId);

            assertEquals("success", result.get("status"));
            assertEquals(true, result.get("isNewTab"));
            assertEquals("tab-2", result.get("newTabId"));
            assertEquals("New Tab", result.get("tabTitle"));
        }
    }

    @Test
    public void testGetReactorDescription() {
        String description = reactor.getReactorDescription();
        assertNotNull(description);
        assertTrue(description.contains("stepId") || description.contains("step"));
    }

    @Test
    public void testGetDescriptionForKey_SessionId() {
        String description = reactor.getDescriptionForKey("sessionId");
        assertNotNull(description);
        assertTrue(description.contains("session") || description.contains("playwright"));
    }

    @Test
    public void testGetDescriptionForKey_FileName() {
        String description = reactor.getDescriptionForKey("fileName");
        assertNotNull(description);
        assertTrue(description.contains("file") || description.contains("recorder"));
    }

    @Test
    public void testGetDescriptionForKey_StepId() {
        String description = reactor.getDescriptionForKey("stepId");
        assertNotNull(description);
        assertTrue(description.contains("step") || description.contains("played"));
    }

    @Test
    public void testGetDescriptionForKey_TabId() {
        String description = reactor.getDescriptionForKey("tabId");
        assertNotNull(description);
        assertTrue(description.contains("tab") || description.contains("playwright"));
    }

    @Test
    public void testExecute_InvalidStepIdFormat_ThrowsException() {
        reactor.keyValue.put("project", "project123");
        reactor.keyValue.put("sessionId", "session123");
        reactor.keyValue.put("fileName", "recording.json");
        reactor.keyValue.put("stepId", "not-a-number");
        reactor.keyValue.put("tabId", "tab-1");

        assertThrows(NumberFormatException.class, () -> reactor.execute());
    }

    @Test
    public void testExecute_MissingRequiredKeys_ThrowsException() {
        reactor.keyValue.put("project", "project123");
        assertThrows(IllegalArgumentException.class, () -> reactor.execute());
    }

    @Test
    public void testReplayStep_WithInputsForTypeStep() {
        String projectId = "project123";
        String sessionId = "session123";
        String fileName = "recording.json";
        int stepId = 1;
        String tabId = "tab-1";
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("username", "testuser");

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class);
             MockedStatic<PlaywrightSessionUtility> mockedSessionUtility = mockStatic(PlaywrightSessionUtility.class);
             MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {

            StepsEnvelope mockEnvelope = mock(StepsEnvelope.class);
            PlaywrightStep typeStep = mock(PlaywrightStep.class);
            when(typeStep.id()).thenReturn(stepId);
            when(typeStep.type()).thenReturn(PlaywrightStepType.TYPE);
            when(typeStep.label()).thenReturn("username");

            List<PlaywrightStep> page = Collections.singletonList(typeStep);
            List<List<PlaywrightStep>> pages = Collections.singletonList(page);
            Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
            stepsMap.put(tabId, pages);

            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile(projectId, fileName))
                    .thenReturn(mockEnvelope);
            when(mockEnvelope.steps()).thenReturn(stepsMap);
            when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

            Map<String, Object> executionResult = new HashMap<>();
            executionResult.put("shouldStop", false);
            executionResult.put("isNewTab", false);

            mockedSessionUtility.when(() -> PlaywrightSessionUtility.applyStep(any(), any(), anyString()))
                    .thenReturn(executionResult);

            ScreenshotResponse mockScreenshotObj = mock(ScreenshotResponse.class);
            mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
                    .thenReturn(mockScreenshotObj);

            Map<String, Object> result = reactor.replayStep(projectId, sessionId, fileName, stepId, inputs, tabId);

            assertEquals("success", result.get("status"));
            assertNotNull(result.get("screenshot"));
        }
    }

    @Test
    public void testReplayStep_WithNavigateStep() {
        String projectId = "project123";
        String sessionId = "session123";
        String fileName = "recording.json";
        int stepId = 1;
        String tabId = "tab-1";

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class);
             MockedStatic<PlaywrightSessionUtility> mockedSessionUtility = mockStatic(PlaywrightSessionUtility.class);
             MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {

            StepsEnvelope mockEnvelope = mock(StepsEnvelope.class);
            PlaywrightStep navStep = mock(PlaywrightStep.class);
            when(navStep.id()).thenReturn(stepId);
            when(navStep.type()).thenReturn(PlaywrightStepType.NAVIGATE);

            List<PlaywrightStep> page = Collections.singletonList(navStep);
            List<List<PlaywrightStep>> pages = Collections.singletonList(page);
            Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
            stepsMap.put(tabId, pages);

            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile(projectId, fileName))
                    .thenReturn(mockEnvelope);
            when(mockEnvelope.steps()).thenReturn(stepsMap);
            when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

            Map<String, Object> executionResult = new HashMap<>();
            executionResult.put("shouldStop", false);
            executionResult.put("isNewTab", false);
            executionResult.put("tabTitle", "Example Page");

            mockedSessionUtility.when(() -> PlaywrightSessionUtility.applyStep(any(), any(), anyString()))
                    .thenReturn(executionResult);

            ScreenshotResponse mockScreenshotObj = mock(ScreenshotResponse.class);
            mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
                    .thenReturn(mockScreenshotObj);

            Map<String, Object> result = reactor.replayStep(projectId, sessionId, fileName, stepId, null, tabId);

            assertEquals("success", result.get("status"));
            assertEquals("Example Page", result.get("tabTitle"));
        }
    }

    @Test
    public void testReplayStep_ExceptionDuringExecution_CapturesScreenshot() {
        String projectId = "project123";
        String sessionId = "session123";
        String fileName = "recording.json";
        int stepId = 1;
        String tabId = "tab-1";

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class);
             MockedStatic<PlaywrightSessionUtility> mockedSessionUtility = mockStatic(PlaywrightSessionUtility.class);
             MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {

            StepsEnvelope mockEnvelope = mock(StepsEnvelope.class);
            Map<String, List<List<PlaywrightStep>>> stepsMap = createMockStepsMap(1);

            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile(projectId, fileName))
                    .thenReturn(mockEnvelope);
            when(mockEnvelope.steps()).thenReturn(stepsMap);
            when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);
            mockSession.tabPages = new HashMap<>();
            mockSession.tabPages.put(tabId, null);

            mockedSessionUtility.when(() -> PlaywrightSessionUtility.applyStep(any(), any(), anyString()))
                    .thenThrow(new RuntimeException("Execution error"));

            ScreenshotResponse mockScreenshotObj = mock(ScreenshotResponse.class);
            mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
                    .thenReturn(mockScreenshotObj);

            Map<String, Object> result = reactor.replayStep(projectId, sessionId, fileName, stepId, null, tabId);

            assertEquals("failed", result.get("status"));
            assertTrue(result.get("error").toString().contains("Execution error"));
            assertNotNull(result.get("screenshot"));
            assertEquals(false, result.get("isNewTab"));
        }
    }

    @Test
    public void testReplayStep_WithShouldStopTrue() {
        String projectId = "project123";
        String sessionId = "session123";
        String fileName = "recording.json";
        int stepId = 1;
        String tabId = "tab-1";

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class);
             MockedStatic<PlaywrightSessionUtility> mockedSessionUtility = mockStatic(PlaywrightSessionUtility.class);
             MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {

            StepsEnvelope mockEnvelope = mock(StepsEnvelope.class);
            Map<String, List<List<PlaywrightStep>>> stepsMap = createMockStepsMap(1);

            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile(projectId, fileName))
                    .thenReturn(mockEnvelope);
            when(mockEnvelope.steps()).thenReturn(stepsMap);
            when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

            Map<String, Object> executionResult = new HashMap<>();
            executionResult.put("shouldStop", true);
            executionResult.put("isNewTab", false);

            mockedSessionUtility.when(() -> PlaywrightSessionUtility.applyStep(any(), any(), anyString()))
                    .thenReturn(executionResult);

            ScreenshotResponse mockScreenshotObj = mock(ScreenshotResponse.class);
            mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
                    .thenReturn(mockScreenshotObj);

            Map<String, Object> result = reactor.replayStep(projectId, sessionId, fileName, stepId, null, tabId);

            assertEquals("success", result.get("status"));
            assertEquals(true, result.get("shouldStop"));
        }
    }

    @Test
    public void testKeysConfiguration() {
        assertNotNull(reactor.keysToGet);
        assertEquals(6, reactor.keysToGet.length);
    }

    @Test
    public void testReplayStep_ExceptionDuringLoadingFile() {
        String projectId = "project123";
        String sessionId = "session123";
        String fileName = "invalid.json";
        int stepId = 1;

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class)) {
            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile(projectId, fileName))
                    .thenThrow(new RuntimeException("File not found"));

            Map<String, Object> result = reactor.replayStep(projectId, sessionId, fileName, stepId, null, null);

            assertEquals("failed", result.get("status"));
            assertTrue(result.get("error").toString().contains("File not found"));
            assertEquals(false, result.get("isNewTab"));
        }
    }

    @Test
    public void testReplayStep_ExecutionResultNull_ReturnsFailed() {
        String projectId = "project123";
        String sessionId = "session123";
        String fileName = "recording.json";
        int stepId = 1;
        String tabId = "tab-1";

        try (MockedStatic<PlaywrightUtility> mockedUtility = mockStatic(PlaywrightUtility.class);
             MockedStatic<PlaywrightSessionUtility> mockedSessionUtility = mockStatic(PlaywrightSessionUtility.class);
             MockedStatic<ScreenshotReactor> mockedScreenshot = mockStatic(ScreenshotReactor.class)) {

            StepsEnvelope mockEnvelope = mock(StepsEnvelope.class);
            Map<String, List<List<PlaywrightStep>>> stepsMap = createMockStepsMap(1);

            mockedUtility.when(() -> PlaywrightUtility.loadStepsFromFile(projectId, fileName))
                    .thenReturn(mockEnvelope);
            when(mockEnvelope.steps()).thenReturn(stepsMap);
            when(mockUser.getPlaywrightSession(sessionId)).thenReturn(mockSession);

            mockedSessionUtility.when(() -> PlaywrightSessionUtility.applyStep(any(), any(), anyString()))
                    .thenReturn(null);

            ScreenshotResponse mockScreenshotObj = mock(ScreenshotResponse.class);
            mockedScreenshot.when(() -> ScreenshotReactor.screenshot(mockSession, tabId))
                    .thenReturn(mockScreenshotObj);

            Map<String, Object> result = reactor.replayStep(projectId, sessionId, fileName, stepId, null, tabId);

            assertEquals("failed", result.get("status"));
            assertEquals("Step execution failed", result.get("error"));
            assertEquals(false, result.get("isNewTab"));
        }
    }

    private Map<String, List<List<PlaywrightStep>>> createMockStepsMap(int stepId) {
        PlaywrightStep step = mock(PlaywrightStep.class);
        when(step.id()).thenReturn(stepId);
        when(step.type()).thenReturn(PlaywrightStepType.CLICK);

        List<PlaywrightStep> page = Collections.singletonList(step);
        List<List<PlaywrightStep>> pages = Collections.singletonList(page);
        Map<String, List<List<PlaywrightStep>>> stepsMap = new HashMap<>();
        stepsMap.put("tab-1", pages);
        return stepsMap;
    }
}

