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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import prerna.reactor.agent.stream.AgentStreamItems;
import prerna.security.HttpStatusException;

class WorkOrgChartUnitTests {

	private static final String GRAPH = "https://graph.microsoft.com/v1.0";
	private static final Map<String, Object> FOCUS = person(1, "Avery Jones");
	private static final Map<String, Object> MANAGER = person(2, "Pat Smith");
	private static final Map<String, Object> PEER = person(3, "Robin Mills");

	private static Map<String, Object> person(int n, String name) {
		return Map.of("id", "00000000-0000-4000-8000-%012d".formatted(n), "displayName", name,
				"mail", "user" + n + "@example.org", "jobTitle", "Analyst", "department", "Operations");
	}

	@Test
	void looksUpTheSignedInUserAndPagesTheManagersReportsToFindPeers() {
		String manager = "/v1.0/users/" + FOCUS.get("id") + "/manager";
		String reports = "/v1.0/users/" + FOCUS.get("id") + "/directReports";
		String peers = "/v1.0/users/" + MANAGER.get("id") + "/directReports";
		String next = "https://graph.microsoft.com" + peers + "?$skiptoken=next";
		Map<String, Object> chart = WorkOrgChart.lookup("me", query -> List.of(), url -> {
			String path = URI.create(url).getPath();
			if (url.equals(next)) return Map.of("value", List.of(PEER));
			if (path.equals("/v1.0/me")) return FOCUS;
			if (path.equals(manager)) return MANAGER;
			if (path.equals(reports)) return Map.of("value", List.of(person(4, "Casey Turner")));
			if (path.equals(peers)) return Map.of("value", List.of(FOCUS), "@odata.nextLink", next);
			throw new AssertionError("Unexpected lookup " + url);
		});
		assertEquals("complete", chart.get("managerStatus"));
		assertEquals("Pat Smith", ((Map<?, ?>) chart.get("manager")).get("name"));
		assertEquals("complete", chart.get("peersStatus"));
		List<?> foundPeers = (List<?>) chart.get("peers");
		assertEquals(1, foundPeers.size());
		assertEquals(PEER.get("id"), ((Map<?, ?>) foundPeers.getFirst()).get("id"));
		assertEquals("complete", chart.get("directReportsStatus"));
		Map<?, ?> report = (Map<?, ?>) ((List<?>) chart.get("directReports")).getFirst();
		assertEquals("Casey Turner", report.get("name"));
		assertEquals("Analyst", report.get("title"));
		assertEquals("Operations", report.get("department"));
	}

	@Test
	void distinguishesAnAbsentManagerFromDirectoryAccessBeingUnavailable() {
		for (int managerStatus : List.of(404, 403)) {
			Map<String, Object> chart = WorkOrgChart.lookup("me", query -> List.of(), url -> {
				String path = URI.create(url).getPath();
				if (path.equals("/v1.0/me")) return FOCUS;
				if (path.endsWith("/manager")) throw new HttpStatusException("manager", managerStatus);
				if (path.endsWith("/directReports")) throw new HttpStatusException("reports", 403);
				throw new AssertionError("Unexpected lookup " + url);
			});
			String expected = managerStatus == 404 ? "not_listed" : "unavailable";
			assertEquals(expected, chart.get("managerStatus"));
			assertEquals(expected, chart.get("peersStatus"));
			assertEquals("unavailable", chart.get("directReportsStatus"));
		}
	}

	@Test
	void asksWhichPersonWhenNamesMatchMultiplePeopleAndBoundsTheChoices() {
		List<Map<String, Object>> matches = IntStream.rangeClosed(1, 11)
				.mapToObj(i -> person(i, "Alex Chen")).toList();
		Map<String, Object> result = WorkOrgChart.lookup("Alex Chen", query -> List.of(), url -> {
			assertEquals("/v1.0/users", URI.create(url).getPath());
			return Map.of("value", matches);
		});
		assertEquals("ambiguous", result.get("status"));
		assertEquals(10, ((List<?>) result.get("candidates")).size());
		assertEquals(true, result.get("candidatesTruncated"));
		assertFalse(result.containsKey("manager"));
	}

	@Test
	void disambiguatesAContactFromADirectoryNamesakeAndMergesTheSameAddress() {
		List<Map<String, Object>> contacts = List.of(
				Map.of("name", "Avery Jones", "email", "USER1@EXAMPLE.ORG"),
				Map.of("name", "Avery Jones", "email", "external@example.net"));
		Map<String, Object> result = WorkOrgChart.lookup("Avery Jones", query -> contacts, url -> {
			assertEquals("/v1.0/users", URI.create(url).getPath());
			return Map.of("value", List.of(FOCUS));
		});
		assertEquals("ambiguous", result.get("status"));
		assertEquals(List.of(FOCUS.get("mail"), "external@example.net"), ((List<?>) result.get("candidates")).stream()
				.map(person -> ((Map<?, ?>) person).get("email")).toList());
	}

	@Test
	void resolvesAnExactEmailOrSignInAndDistinguishesNoDirectoryMatch() {
		String address = "o'brien@example.org";
		Map<String, Object> result = WorkOrgChart.lookup(address, query -> {
			throw new AssertionError("An exact address must not use fuzzy contact matches");
		}, url -> {
			URI uri = URI.create(url);
			assertEquals("/v1.0/users", uri.getPath());
			assertTrue(uri.getQuery().contains("mail eq 'o''brien@example.org' or userPrincipalName eq 'o''brien@example.org'"));
			return Map.of("value", List.of());
		});
		assertEquals("not_found", result.get("status"));
	}

	@Test
	void preservesTheFirstPageWhenTheNextPageCannotBeRead() {
		Map<String, Object> result = WorkOrgChart.lookup("me", query -> List.of(), url -> {
			String path = URI.create(url).getPath();
			if (path.equals("/v1.0/me")) return FOCUS;
			if (path.endsWith("/manager")) throw new HttpStatusException("not listed", 404);
			if (url.contains("$skiptoken")) throw new HttpStatusException("unavailable", 503);
			if (path.endsWith("/directReports")) return Map.of("value", List.of(PEER),
					"@odata.nextLink", "https://graph.microsoft.com" + path + "?$skiptoken=next");
			throw new AssertionError("Unexpected lookup " + url);
		});
		assertEquals("partial", result.get("directReportsStatus"));
		List<?> reports = (List<?>) result.get("directReports");
		assertEquals(1, reports.size());
		assertEquals(PEER.get("id"), ((Map<?, ?>) reports.getFirst()).get("id"));
	}

	@Test
	void stopsPagingAtTheLimitAndMarksIncompleteReports() {
		List<Map<String, Object>> reports = IntStream.rangeClosed(30, 55)
				.mapToObj(i -> person(i, "Report " + i)).toList();
		Map<String, Object> chart = WorkOrgChart.lookup("me", query -> List.of(), url -> {
			String path = URI.create(url).getPath();
			if (path.equals("/v1.0/me")) return FOCUS;
			if (path.endsWith("/manager")) throw new HttpStatusException("not listed", 404);
			if (path.endsWith("/directReports")) return Map.of("value", reports, "@odata.nextLink", GRAPH + "/never");
			throw new AssertionError("Unexpected lookup " + url);
		});
		assertEquals("partial", chart.get("directReportsStatus"));
		List<?> shown = (List<?>) chart.get("directReports");
		assertEquals(25, shown.size());
	}

	@Test
	void longNamesStillProduceAnIntactInlineChart() {
		String longName = "Pe\u00f1a \"Operations\" ".repeat(10);
		List<Map<String, Object>> people = IntStream.range(30, 55).mapToObj(i -> person(i, longName)).toList();
		Map<String, Object> chart = WorkOrgChart.lookup("me", query -> List.of(), url -> {
			String path = URI.create(url).getPath();
			if (path.equals("/v1.0/me")) return FOCUS;
			if (path.endsWith("/manager")) return MANAGER;
			if (path.endsWith("/directReports")) return Map.of("value", people);
			throw new AssertionError("Unexpected lookup " + url);
		});
		assertTrue(CollaborationDbUtils.toJson(chart).length() <= AgentStreamItems.MAX_TOOL_OUTPUT_CHARS);
		assertEquals("partial", chart.get("directReportsStatus"));
		assertEquals("partial", chart.get("peersStatus"));
		assertEquals(FOCUS.get("id"), ((Map<?, ?>) chart.get("person")).get("id"));
		assertEquals(MANAGER.get("id"), ((Map<?, ?>) chart.get("manager")).get("id"));
	}
}
