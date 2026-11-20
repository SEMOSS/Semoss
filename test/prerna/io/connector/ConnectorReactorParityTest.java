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
package prerna.io.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;

/**
 * Holds the mail and calendar reactors of every provider to one contract.
 *
 * <p>
 * The compiler already keeps a provider from changing what an operation
 * abstract settles. What it cannot see is a provider that has no reactor for an
 * operation, or one that reassigns its keys, so this checks the tools the
 * reactors actually advertise: one reactor per provider for every operation,
 * and the same keys, types, descriptions, execution and view for each pair.
 * </p>
 */
class ConnectorReactorParityTest {

	/**
	 * Keys only one provider of an operation takes, because the other has no such
	 * thing.
	 */
	private static final Set<String> EXTRA_KEYS = Set.of("saveToSentItems", "importance");

	/**
	 * Keys every provider takes, whose accepted values, and so descriptions,
	 * differ.
	 */
	private static final Set<String> PROVIDER_CHECKED_KEYS = Set.of("showAs", "categories");

	/** What a description names that differs by provider, and nothing else may. */
	private static final String[] APP_WORDS = { "MicrosoftOutlook", "GoogleGmail", "MicrosoftCalendar",
			"GoogleCalendar", "Outlook mailbox", "Gmail mailbox", "Microsoft 365 calendar", "Google Calendar" };

	@Test
	void everyOperationHasAReactorForEveryProvider() {
		Map<Class<?>, List<Class<?>>> operations = operations();
		assertFalse(operations.isEmpty(), "No mail or calendar operations were found");
		for (Map.Entry<Class<?>, List<Class<?>>> operation : operations.entrySet()) {
			List<String> names = operation.getValue().stream().map(Class::getSimpleName).sorted().toList();
			assertEquals(2, names.size(), operation.getKey().getSimpleName() + " has reactors " + names);
			assertTrue(names.stream().anyMatch(name -> name.startsWith("Microsoft")),
					operation.getKey().getSimpleName() + " has no Microsoft reactor: " + names);
			assertTrue(names.stream().anyMatch(name -> name.startsWith("Google")),
					operation.getKey().getSimpleName() + " has no Google reactor: " + names);
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("pairs")
	void bothProvidersAdvertiseTheSameTool(String operation, Class<?> microsoft, Class<?> google) throws Exception {
		JSONObject microsoftTool = tool(microsoft);
		JSONObject googleTool = tool(google);

		assertEquals(normalize(microsoftTool.getString("description")), normalize(googleTool.getString("description")),
				"descriptions of " + operation);

		JSONObject microsoftKeys = microsoftTool.getJSONObject("inputSchema").getJSONObject("properties");
		JSONObject googleKeys = googleTool.getJSONObject("inputSchema").getJSONObject("properties");
		Set<String> onlyOne = new HashSet<>(microsoftKeys.keySet());
		onlyOne.addAll(googleKeys.keySet());
		Set<String> shared = new HashSet<>(microsoftKeys.keySet());
		shared.retainAll(googleKeys.keySet());
		onlyOne.removeAll(shared);
		assertTrue(EXTRA_KEYS.containsAll(onlyOne), operation + " has keys only one provider takes: " + onlyOne);

		for (String key : shared) {
			JSONObject microsoftKey = microsoftKeys.getJSONObject(key);
			JSONObject googleKey = googleKeys.getJSONObject(key);
			assertEquals(microsoftKey.getString("type"), googleKey.getString("type"), operation + " " + key);
			assertEquals(String.valueOf(microsoftKey.opt("items")), String.valueOf(googleKey.opt("items")),
					operation + " " + key);
			if (!PROVIDER_CHECKED_KEYS.contains(key)) {
				assertEquals(normalize(microsoftKey.getString("description")),
						normalize(googleKey.getString("description")), operation + " " + key);
			}
		}

		assertEquals(required(microsoftTool), required(googleTool), "required keys of " + operation);

		JSONObject microsoftMeta = microsoftTool.getJSONObject("_meta");
		JSONObject googleMeta = googleTool.getJSONObject("_meta");
		assertEquals(microsoftMeta.getString(MCPUtility.SMSS_MCP_EXECUTION),
				googleMeta.getString(MCPUtility.SMSS_MCP_EXECUTION), "execution of " + operation);
		JSONObject microsoftUi = microsoftMeta.getJSONObject(MCPUtility.SMSS_MCP_UI);
		JSONObject googleUi = googleMeta.getJSONObject(MCPUtility.SMSS_MCP_UI);
		assertEquals(microsoftUi.optString(MCPUtility.UI_DISPLAY_LOCATION),
				googleUi.optString(MCPUtility.UI_DISPLAY_LOCATION), "display location of " + operation);
		String microsoftView = microsoftUi.optString(MCPUtility.UI_RESOURCE_URI, null);
		String googleView = googleUi.optString(MCPUtility.UI_RESOURCE_URI, null);
		if (microsoftView == null || googleView == null) {
			assertEquals(microsoftView, googleView, "view of " + operation);
		} else {
			assertTrue(microsoftView.startsWith(MCPUtility.UI_COMPONENT_SCHEME), microsoftView);
			assertTrue(microsoftView.endsWith("provider=microsoft"), microsoftView);
			assertTrue(googleView.endsWith("provider=google"), googleView);
			assertEquals(microsoftView.replace("provider=microsoft", "provider="),
					googleView.replace("provider=google", "provider="), "view of " + operation);
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("pairs")
	void toolsThatAskTellTheModelTheUserMayEdit(String operation, Class<?> microsoft, Class<?> google)
			throws Exception {
		for (Class<?> reactor : List.of(microsoft, google)) {
			JSONObject tool = tool(reactor);
			boolean asks = MCPUtility.MCPExecution.ASK.getValue()
					.equals(tool.getJSONObject("_meta").getString(MCPUtility.SMSS_MCP_EXECUTION));
			assertEquals(asks, tool.getString("description").endsWith(AbstractConnectorAppReactor.ASK_NOTE),
					reactor.getSimpleName());
		}
	}

	private static List<Arguments> pairs() {
		List<Arguments> pairs = new ArrayList<>();
		for (Map.Entry<Class<?>, List<Class<?>>> operation : operations().entrySet()) {
			Class<?> microsoft = null;
			Class<?> google = null;
			for (Class<?> reactor : operation.getValue()) {
				if (reactor.getSimpleName().startsWith("Microsoft")) {
					microsoft = reactor;
				} else if (reactor.getSimpleName().startsWith("Google")) {
					google = reactor;
				}
			}
			if (microsoft != null && google != null) {
				pairs.add(Arguments.of(operation.getKey().getSimpleName(), microsoft, google));
			}
		}
		return pairs;
	}

	/**
	 * Every operation abstract, which is an abstract mail or calendar reactor no
	 * other abstract extends, with the concrete reactors of it.
	 */
	private static Map<Class<?>, List<Class<?>>> operations() {
		Map<Class<?>, List<Class<?>>> operations = new LinkedHashMap<>();
		try (ScanResult scan = new ClassGraph().enableClassInfo().acceptPackages("prerna.io.connector").scan()) {
			for (ClassInfo info : scan.getSubclasses(AbstractConnectorAppReactor.class.getName())) {
				String packageName = info.getPackageName();
				boolean shared = packageName.equals("prerna.io.connector.mail")
						|| packageName.equals("prerna.io.connector.calendar");
				if (shared && info.isAbstract() && info.getSubclasses().filter(ClassInfo::isAbstract).isEmpty()) {
					operations.put(info.loadClass(),
							info.getSubclasses().filter(subclass -> !subclass.isAbstract()).loadClasses());
				}
			}
		}
		return operations;
	}

	private static JSONObject tool(Class<?> reactor) throws Exception {
		return ((AbstractReactor) reactor.getDeclaredConstructor().newInstance()).asMcpTool();
	}

	private static Set<String> required(JSONObject tool) {
		JSONArray required = tool.getJSONObject("inputSchema").getJSONArray("required");
		Set<String> keys = new HashSet<>();
		for (int i = 0; i < required.length(); i++) {
			keys.add(required.getString(i));
		}
		keys.removeAll(EXTRA_KEYS);
		return keys;
	}

	private static String normalize(String description) {
		String normalized = description;
		for (String word : APP_WORDS) {
			normalized = normalized.replace(word, "<app>");
		}
		return normalized;
	}
}
