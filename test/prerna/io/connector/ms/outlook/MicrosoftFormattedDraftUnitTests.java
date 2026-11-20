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
package prerna.io.connector.ms.outlook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.security.HttpHelperUtility;

class MicrosoftFormattedDraftUnitTests {

	private final Gson gson = new Gson();

	@Test
	void replyReplyAllAndForwardPatchOnlyNewDraftAndPreserveNativeQuote() {
		for (String action : List.of("reply", "replyAll", "forward")) {
			List<String> calls = new ArrayList<>();
			try (MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class, invocation -> {
				String method = invocation.getMethod().getName();
				String url = invocation.getArgument(0);
				calls.add(method + " " + url);
				if (method.equals("postRequestStringBody")) {
					String request = invocation.getArgument(2);
					assertFalse(request.contains("comment"));
					if (action.equals("forward")) {
						assertTrue(request.contains("person@example.com"));
					}
					return gson.toJson(Map.of("id", "new-draft", "webLink", "https://outlook.office.com/draft", "body",
							Map.of("contentType", "HTML", "content",
									"<html><head><style>p{color:blue}</style></head><body><blockquote>Native quoted body</blockquote></body></html>")));
				}
				assertEquals("patchRequestStringBody", method);
				assertTrue(url.endsWith("/messages/new-draft"));
				String request = invocation.getArgument(2);
				assertTrue(request.contains("<strong>Authored</strong>"));
				assertTrue(request.contains("Native quoted body"));
				assertTrue(request.indexOf("Authored") < request.indexOf("Native quoted body"));
				assertTrue(request.contains("color:blue"));
				return "{\"id\":\"new-draft\"}";
			})) {
				MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();
				Map<String, Object> result = action.equals("forward")
						? helper.forwardHtmlDraft("test-token", "source-id", new String[] { "person@example.com" },
								"<p><strong>Authored</strong></p>")
						: helper.replyHtmlDraft("test-token", "source-id", "<p><strong>Authored</strong></p>",
								action.equals("replyAll"));
				assertEquals("new-draft", result.get("id"));
				assertEquals("https://outlook.office.com/draft", result.get("webLink"));
				assertEquals(2, calls.size());
				assertTrue(calls.get(0).endsWith(action.equals("forward") ? "/createForward"
						: action.equals("replyAll") ? "/createReplyAll" : "/createReply"));
				assertTrue(calls.stream().noneMatch(call -> call.endsWith("/send")));
			}
		}
	}

	@Test
	void readsMissingQuotedBodyThenUpdatesWithoutFlattening() {
		List<String> calls = new ArrayList<>();
		try (MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class, invocation -> {
			String method = invocation.getMethod().getName();
			calls.add(method);
			if (method.equals("postRequestStringBody")) {
				return "{\"id\":\"new-draft\"}";
			}
			if (method.equals("getRequest")) {
				return "{\"id\":\"new-draft\",\"body\":{\"contentType\":\"text\",\"content\":\"Earlier <literal>\\n  code\"}}";
			}
			assertEquals("patchRequestStringBody", method);
			String request = invocation.getArgument(2);
			assertTrue(request.contains("&lt;literal&gt;"));
			assertTrue(request.contains("  code"));
			return "{\"id\":\"new-draft\"}";
		})) {
			new MicrosoftOutlookMailHelper().replyHtmlDraft("token", "original", "<p>Reply</p>", false);
			assertEquals(List.of("postRequestStringBody", "getRequest", "patchRequestStringBody"), calls);
		}
	}

	@Test
	void updateFailureNeverReportsSuccessOrCreatesAnotherDraft() {
		List<String> calls = new ArrayList<>();
		try (MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class, invocation -> {
			String method = invocation.getMethod().getName();
			calls.add(method);
			if (method.equals("postRequestStringBody")) {
				return "{\"id\":\"created\",\"body\":{\"contentType\":\"html\",\"content\":\"<p>Quote</p>\"}}";
			}
			return "{\"error\":{\"code\":\"ErrorAccessDenied\",\"message\":\"Cannot update\"}}";
		})) {
			assertThrows(IllegalArgumentException.class,
					() -> new MicrosoftOutlookMailHelper().replyHtmlDraft("token", "source", "<p>Reply</p>", false));
			assertEquals(List.of("postRequestStringBody", "patchRequestStringBody"), calls);
		}
	}

	@Test
	void newDraftKeepsHtmlAndNeverSends() {
		List<String> calls = new ArrayList<>();
		try (MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class, invocation -> {
			calls.add(invocation.getArgument(0));
			String request = invocation.getArgument(2);
			assertTrue(request.contains("<strong>New</strong>"));
			return "{\"id\":\"new\"}";
		})) {
			new MicrosoftOutlookMailHelper().createDraft("token", null,
					Map.of("body", Map.of("contentType", "HTML", "content", "<p><strong>New</strong></p>")));
			assertEquals(1, calls.size());
			assertTrue(calls.get(0).endsWith("/messages"));
		}
	}
}
