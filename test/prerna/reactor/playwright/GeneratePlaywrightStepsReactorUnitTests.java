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
// mvn -q -Dtest=GeneratePlaywrightStepsReactorUnitTests test

package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

@Execution(ExecutionMode.SAME_THREAD)
class GeneratePlaywrightStepsReactorUnitTests {

    private GeneratePlaywrightStepsReactor reactor;
    private Map<String, String> keyValues;
    private NounStore nounStore;
    private Insight insight;

    @BeforeEach
    void setUp() {
        reactor = new GeneratePlaywrightStepsReactor();
        keyValues = reactor.keyValue;
        nounStore = new NounStore("playwright-test");
        insight = mock(Insight.class);

        reactor.setNounStore(nounStore);
        reactor.setInsight(insight);
    }

    @Test
    void execute_happyPath_returnsStepsJson() {
        keyValues.put(ReactorKeysEnum.ENGINE.getKey(), "engine-1");
        keyValues.put("sessionId", "session-1");
        keyValues.put(ReactorKeysEnum.ROOM_ID.getKey(), "room-1");

        Map<String, Object> extractionData = new HashMap<>();
        extractionData.put("elementCount", 1);
        extractionData.put("summary", Map.of("hasForm", true));
        extractionData.put("elements", List.of(
                Map.of("interactive", true, "selector", "#username", "aria-label", "Username"),
                Map.of("interactive", false, "selector", "#ignored")));

        Map<String, Object> params = new HashMap<>();
        params.put("extractionData", extractionData);
        params.put("cropParams", Map.of("startX", 0, "startY", 0, "endX", 10, "endY", 10));
        addParamsToStore(params);

        User user = mock(User.class);
        PlaywrightSession session = mock(PlaywrightSession.class);
        when(insight.getUser()).thenReturn(user);
        when(user.getPlaywrightSession("session-1")).thenReturn(session);
        when(insight.getInsightFolder()).thenReturn("/tmp/insight");

        ScreenshotResponse screenshot = new ScreenshotResponse("AA==", 10, 10, 1.0);
        IModelEngine modelEngine = mock(IModelEngine.class);

        try (MockedStatic<ScreenshotReactor> screenshotMock = Mockito.mockStatic(ScreenshotReactor.class);
                MockedStatic<Utility> utilityMock = Mockito.mockStatic(Utility.class);
                MockedStatic<PlaywrightUtility> utility = Mockito.mockStatic(PlaywrightUtility.class)) {

            screenshotMock.when(() -> ScreenshotReactor.croppedScreenshot(eq(session), eq("tab-1"), anyInt(), anyInt(), anyInt(), anyInt()))
                    .thenReturn(screenshot);
            utilityMock.when(() -> Utility.getModel("engine-1")).thenReturn(modelEngine);
            utility.when(() -> PlaywrightUtility.callModel(anyString(), anyString(), eq(screenshot), eq(modelEngine), anyString(), eq(insight), anyString()))
                    .thenReturn("```json\n[{\"type\":\"CLICK\"}]\n```");

            NounMetadata result = reactor.execute();
            assertEquals(PixelDataType.MAP, result.getNounType());

            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) result.getValue();
            assertEquals(Boolean.TRUE, payload.get("success"));
            assertEquals("[{\"type\":\"CLICK\"}]", payload.get("stepsJson"));
        }
    }

    @Test
    void execute_missingExtractionData_returnsError() {
        keyValues.put(ReactorKeysEnum.ENGINE.getKey(), "engine-1");
        keyValues.put("sessionId", "session-1");
        addParamsToStore(new HashMap<>());

        NounMetadata result = reactor.execute();
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) result.getValue();
        assertEquals(Boolean.FALSE, payload.get("success"));
        assertTrue(((String) payload.get("error")).contains("extractionData"));
    }

    private void addParamsToStore(Map<String, Object> params) {
        GenRowStruct grs = new GenRowStruct();
        grs.add(new NounMetadata(params, PixelDataType.MAP));
        nounStore.addNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), grs);
    }
}
