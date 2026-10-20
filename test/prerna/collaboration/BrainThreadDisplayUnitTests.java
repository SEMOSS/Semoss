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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class BrainThreadDisplayUnitTests {
    private Map<String,Object> read(String source, String decision, boolean display, boolean keywordHidden, boolean missing, AtomicInteger fetches) {
        try (MockedStatic<CollaborationDbUtils> db = mockStatic(CollaborationDbUtils.class, invocation -> {
            String method = invocation.getMethod().getName();
            if (method.equals("queryOne")) return missing ? null : new String[]{source, source + ":conversation", "false"};
            if (method.equals("query")) {
                String sql = invocation.getArgument(0);
                if (!sql.contains("FROM BRAIN_MESSAGE")) return List.of();
                ResultSet rs = mock(ResultSet.class);
                when(rs.getString("GRAPH_ID")).thenReturn("native-id");
                when(rs.getString("DECISION")).thenReturn(decision);
                CollaborationDbUtils.RowMapper<?> mapper = invocation.getArgument(1);
                return List.of(mapper.map(rs));
            }
            if (method.equals("getTimestamp")) return "2026-09-25T12:00:00Z";
            return invocation.callRealMethod();
        }); MockedStatic<BrainRulesGate> rules = mockStatic(BrainRulesGate.class, invocation -> {
            String method = invocation.getMethod().getName();
            if (method.equals("activeRules")) return List.of();
            if (method.equals("keywordRule")) return keywordHidden ? new BrainRulesGate.Rule("rule", "keyword", "Hidden", null, null, null) : null;
            return invocation.callRealMethod();
        })) {
            BrainMessageSource messages = (user, channel, conversation, id) -> {
                fetches.incrementAndGet();
                return Map.of("body", Map.of("contentType", "html", "content", "<p>Visible</p><blockquote>From: Earlier</blockquote><pre>  code\n    indent</pre>"), "uniqueBody", Map.of("contentType", "text", "content", "Visible"), "webLink", "https://outlook.office.com/mail/native-id");
            };
            return BrainThreadMessages.read(null, "owner", "type", "thread", 20, messages, display);
        }
    }
    @Test void optInKeepsDisplaySeparateAndLegacyDefaultOmitsIt() {
        Map<?,?> legacy = (Map<?,?>)((List<?>)read("outlook", "include", false, false, false, new AtomicInteger()).get("messages")).get(0);
        assertFalse(legacy.containsKey("displayBody"));
        Map<?,?> rich = (Map<?,?>)((List<?>)read("outlook", "exclude", true, false, false, new AtomicInteger()).get("messages")).get(0);
        assertTrue(rich.containsKey("displayBody"));
        assertTrue(rich.get("text") instanceof String);
        assertEquals("https://outlook.office.com/mail/native-id", rich.get("webLink"));
    }
    @Test void neverIngestAndAccessRulesPreventFetchingDisplayBodies() {
        AtomicInteger count = new AtomicInteger();
        assertTrue(((List<?>)read("outlook", BrainRulesGate.NEVER, true, false, false, count).get("messages")).isEmpty());
        assertEquals(0, count.get());
        assertThrows(IllegalArgumentException.class, () -> read("outlook", "include", true, false, true, count));
        assertEquals(0, count.get());
    }
    @Test void keywordRecheckDropsDisplayAfterFetch() {
        AtomicInteger count = new AtomicInteger();
        assertTrue(((List<?>)read("outlook", "include", true, true, false, count).get("messages")).isEmpty());
        assertEquals(1, count.get());
    }
    @Test void teamsPlainContextKeepsQuotesAndCode() {
        Map<?,?> message = (Map<?,?>)((List<?>)read("teams", "include", true, false, false, new AtomicInteger()).get("messages")).get(0);
        assertTrue(message.get("text").toString().contains("From: Earlier"));
        assertTrue(message.get("text").toString().contains("  code\n    indent"));
    }
}
