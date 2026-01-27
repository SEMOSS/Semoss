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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.microsoft.playwright.Page;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class DeleteTabReactorTest {

    private DeleteTabReactor reactor;
    private Insight insight;
    private User user;
    private PlaywrightSession session;
    private Page page;

    @BeforeEach
    void setup() {
        reactor = new DeleteTabReactor();
        insight = mock(Insight.class);
        user = mock(User.class);
        session = mock(PlaywrightSession.class);
        page = mock(Page.class);

        reactor.setInsight(insight);
        when(insight.getUser()).thenReturn(user);
    }

    @Test
    void testDeleteTabSuccessfully() {
        String sessionId = "session123";
        String tabId = "tab-2";

        reactor.keyValue.put("sessionId", sessionId);
        reactor.keyValue.put("tabId", tabId);

        Map<String, List<List<PlaywrightStep>>> steps = new HashMap<>();
        steps.put(tabId, new ArrayList<>());
        StepsEnvelope history = mock(StepsEnvelope.class);
        when(history.steps()).thenReturn(steps);

        session.history = history;
        session.tabPages = new HashMap<>();
        session.tabPages.put(tabId, page);
        session.tabPages.put("tab-1", mock(Page.class));
        session.tabCurrentPageIndex = new HashMap<>();
        session.tabCurrentPageIndex.put(tabId, 0);
        session.tabCurrentStepIndex = new HashMap<>();
        session.tabCurrentStepIndex.put(tabId, 0);

        when(user.getPlaywrightSession(sessionId)).thenReturn(session);
        when(session.getChildTabs(tabId)).thenReturn(new ArrayList<>());
        when(page.isClosed()).thenReturn(false);

        NounMetadata result = reactor.execute();

        assertNotNull(result);
        assertEquals(PixelDataType.MAP, result.getNounType());
        Map<String, Object> response = (Map<String, Object>) result.getValue();
        assertTrue((Boolean) response.get("success"));
        assertEquals(tabId, response.get("deletedTab"));
        verify(page).close();
        verify(session).removeTabRelationships(tabId);
    }

    @Test
    void testDeleteTabWithMissingSessionId() {
        reactor.keyValue.put("sessionId", "");
        reactor.keyValue.put("tabId", "tab-2");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reactor.execute();
        });

        assertEquals("sessionId is required", exception.getMessage());
    }

    @Test
    void testDeleteTabWithNullSessionId() {
        reactor.keyValue.put("tabId", "tab-2");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reactor.execute();
        });

        assertEquals("Required input(s) missing: sessionId", exception.getMessage());
    }

    @Test
    void testDeleteTabWithMissingTabId() {
        reactor.keyValue.put("sessionId", "session123");
        reactor.keyValue.put("tabId", "");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reactor.execute();
        });

        assertEquals("tabId is required", exception.getMessage());
    }

    @Test
    void testDeleteTabWithNullTabId() {
        reactor.keyValue.put("sessionId", "session123");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reactor.execute();
        });

        assertEquals("Required input(s) missing: tabId", exception.getMessage());
    }

    @Test
    void testDeleteTabWithNonExistentSession() {
        String sessionId = "nonExistentSession";
        String tabId = "tab-2";

        reactor.keyValue.put("sessionId", sessionId);
        reactor.keyValue.put("tabId", tabId);

        when(user.getPlaywrightSession(sessionId)).thenReturn(null);

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            reactor.execute();
        });

        assertEquals("Session not found: " + sessionId, exception.getMessage());
    }

    @Test
    void testDeleteTabWithNonExistentTab() {
        String sessionId = "session123";
        String tabId = "nonExistentTab";

        reactor.keyValue.put("sessionId", sessionId);
        reactor.keyValue.put("tabId", tabId);

        Map<String, List<List<PlaywrightStep>>> steps = new HashMap<>();
        StepsEnvelope history = mock(StepsEnvelope.class);
        when(history.steps()).thenReturn(steps);
        session.history = history;

        when(user.getPlaywrightSession(sessionId)).thenReturn(session);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reactor.execute();
        });

        assertEquals("Tab not found in session: " + tabId, exception.getMessage());
    }

    @Test
    void testDeleteTabWithSingleChild() {
        String sessionId = "session123";
        String tabId = "tab-2";
        String childTabId = "tab-3";

        reactor.keyValue.put("sessionId", sessionId);
        reactor.keyValue.put("tabId", tabId);

        Map<String, List<List<PlaywrightStep>>> steps = new HashMap<>();
        steps.put(tabId, new ArrayList<>());
        StepsEnvelope history = mock(StepsEnvelope.class);
        when(history.steps()).thenReturn(steps);
        session.history = history;

        when(user.getPlaywrightSession(sessionId)).thenReturn(session);
        when(session.getChildTabs(tabId)).thenReturn(Arrays.asList(childTabId));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reactor.execute();
        });

        assertEquals("Cannot delete 'tab-2': 1 child tab must be closed first (tab-3)", exception.getMessage());
    }

    @Test
    void testDeleteTabWithMultipleChildren() {
        String sessionId = "session123";
        String tabId = "tab-2";
        List<String> children = Arrays.asList("tab-3", "tab-4", "tab-5");

        reactor.keyValue.put("sessionId", sessionId);
        reactor.keyValue.put("tabId", tabId);

        Map<String, List<List<PlaywrightStep>>> steps = new HashMap<>();
        steps.put(tabId, new ArrayList<>());
        StepsEnvelope history = mock(StepsEnvelope.class);
        when(history.steps()).thenReturn(steps);
        session.history = history;

        when(user.getPlaywrightSession(sessionId)).thenReturn(session);
        when(session.getChildTabs(tabId)).thenReturn(children);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reactor.execute();
        });

        assertEquals("Cannot delete 'tab-2': 3 child tabs must be closed first (tab-3, tab-4, tab-5)",
                exception.getMessage());
    }

    @Test
    void testDeleteTabWithClosedPage() {
        String sessionId = "session123";
        String tabId = "tab-2";

        reactor.keyValue.put("sessionId", sessionId);
        reactor.keyValue.put("tabId", tabId);

        Map<String, List<List<PlaywrightStep>>> steps = new HashMap<>();
        steps.put(tabId, new ArrayList<>());
        StepsEnvelope history = mock(StepsEnvelope.class);
        when(history.steps()).thenReturn(steps);

        session.history = history;
        session.tabPages = new HashMap<>();
        session.tabPages.put(tabId, page);
        session.tabCurrentPageIndex = new HashMap<>();
        session.tabCurrentPageIndex.put(tabId, 0);
        session.tabCurrentStepIndex = new HashMap<>();
        session.tabCurrentStepIndex.put(tabId, 0);

        when(user.getPlaywrightSession(sessionId)).thenReturn(session);
        when(session.getChildTabs(tabId)).thenReturn(new ArrayList<>());
        when(page.isClosed()).thenReturn(true);

        NounMetadata result = reactor.execute();

        assertNotNull(result);
        assertEquals(PixelDataType.MAP, result.getNounType());
        Map<String, Object> response = (Map<String, Object>) result.getValue();
        assertTrue((Boolean) response.get("success"));
        verify(page, never()).close();
    }

    @Test
    void testDeleteTabWithNullPage() {
        String sessionId = "session123";
        String tabId = "tab-2";

        reactor.keyValue.put("sessionId", sessionId);
        reactor.keyValue.put("tabId", tabId);

        Map<String, List<List<PlaywrightStep>>> steps = new HashMap<>();
        steps.put(tabId, new ArrayList<>());
        StepsEnvelope history = mock(StepsEnvelope.class);
        when(history.steps()).thenReturn(steps);

        session.history = history;
        session.tabPages = new HashMap<>();
        session.tabCurrentPageIndex = new HashMap<>();
        session.tabCurrentPageIndex.put(tabId, 0);
        session.tabCurrentStepIndex = new HashMap<>();
        session.tabCurrentStepIndex.put(tabId, 0);

        when(user.getPlaywrightSession(sessionId)).thenReturn(session);
        when(session.getChildTabs(tabId)).thenReturn(new ArrayList<>());

        NounMetadata result = reactor.execute();

        assertNotNull(result);
        assertEquals(PixelDataType.MAP, result.getNounType());
        Map<String, Object> response = (Map<String, Object>) result.getValue();
        assertTrue((Boolean) response.get("success"));
    }

    @Test
    void testDeleteTabWhenOnlyOneTabRemains() {
        String sessionId = "session123";
        String tabId = "tab-2";

        reactor.keyValue.put("sessionId", sessionId);
        reactor.keyValue.put("tabId", tabId);

        Map<String, List<List<PlaywrightStep>>> steps = new HashMap<>();
        steps.put(tabId, new ArrayList<>());
        StepsEnvelope history = mock(StepsEnvelope.class);
        when(history.steps()).thenReturn(steps);

        session.history = history;
        session.tabPages = new HashMap<>();
        session.tabPages.put(tabId, page);
        session.tabCurrentPageIndex = new HashMap<>();
        session.tabCurrentPageIndex.put(tabId, 0);
        session.tabCurrentStepIndex = new HashMap<>();
        session.tabCurrentStepIndex.put(tabId, 0);

        when(user.getPlaywrightSession(sessionId)).thenReturn(session);
        when(session.getChildTabs(tabId)).thenReturn(new ArrayList<>());
        when(page.isClosed()).thenReturn(false);

        NounMetadata result = reactor.execute();

        assertNotNull(result);
        verify(page, never()).close();
    }
}
