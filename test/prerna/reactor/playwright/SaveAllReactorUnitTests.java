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
// mvn -q -Dtest=SaveAllReactorUnitTests test

package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

@Execution(ExecutionMode.SAME_THREAD)
class SaveAllReactorUnitTests {

    private SaveAllReactor reactor;
    private Map<String, String> keyValues;
    private Insight insight;

    @BeforeEach
    void setUp() {
        reactor = new SaveAllReactor();
        keyValues = reactor.keyValue;
        insight = mock(Insight.class);
        reactor.setInsight(insight);
    }

    @Test
    void execute_writesRecordingToProjectDir(@TempDir Path tempDir) throws Exception {
        keyValues.put("project", "proj-1");
        keyValues.put("sessionId", "sess-1");
        keyValues.put("name", "login-script");

        User user = mock(User.class);
        PlaywrightSession session = mock(PlaywrightSession.class);
        session.history = new StepsEnvelope("1", PlaywrightSession.newMeta(""), new HashMap<>());
        session.history.steps().put("tab-1", new ArrayList<>());
        when(insight.getUser()).thenReturn(user);
        when(user.getPlaywrightSession("sess-1")).thenReturn(session);

        try (MockedStatic<PlaywrightUtility> util = Mockito.mockStatic(PlaywrightUtility.class, Mockito.CALLS_REAL_METHODS)) {
            util.when(() -> PlaywrightUtility.initRecordingsDir("proj-1")).thenReturn(tempDir);

            NounMetadata result = reactor.execute();
            assertTrue(result.getNounType() == PixelDataType.CONST_STRING);

            Path saved = Path.of((String) result.getValue());
            assertTrue(saved.startsWith(tempDir));
            assertTrue(Files.exists(saved));
        }
    }

    @Test
    void execute_missingNameThrows() {
        keyValues.put("project", "proj-1");
        keyValues.put("sessionId", "sess-1");

        assertThrows(IllegalArgumentException.class, () -> reactor.execute());
    }
}
