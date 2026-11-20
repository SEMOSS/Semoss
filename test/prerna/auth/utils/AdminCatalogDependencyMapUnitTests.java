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
package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.CatalogDependencyGraphLayout.Direction;
import prerna.auth.utils.SecurityCatalogDependencyUtils.Dependency;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.security.AdminGetCatalogDependencyGraphReactor;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * The admin dependency map's reads against a disposable security database: the
 * whole graph and a traced one, and a user's effective access to every item.
 */
public class AdminCatalogDependencyMapUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();
	private User admin;

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// AdminGetCatalogDependencyGraph
	///

	private void createGraph() throws Exception {
		UnitTestSecurityAuthUtils.createEngine("model1", "Model", IEngine.CATALOG_TYPE.MODEL, admin);
		UnitTestSecurityAuthUtils.createEngine("vector1", "Vector", IEngine.CATALOG_TYPE.VECTOR, admin);
		UnitTestSecurityAuthUtils.createEngine("lonely", "Lonely", IEngine.CATALOG_TYPE.DATABASE, admin);
		UnitTestSecurityAuthUtils.createProjectOfType("agent1", "Agent", "WORKSPACE", admin);
		SecurityEngineUtils.updateEngineMetadata(admin, "vector1", Map.of("description", "Some vectors"));
		// agent1 -> vector1 -> model1, agent1 -> model1 (automatic), agent1 -> gone (a deleted skill)
		SecurityCatalogDependencyUtils.setManualDependencies(admin, "agent1", "WORKSPACE",
				List.of(new Dependency("vector1", "VECTOR")));
		SecurityCatalogDependencyUtils.addAutomaticDependencies(admin, "vector1", "VECTOR",
				List.of(new Dependency("model1", "MODEL", true, CatalogDependencyRole.EMBEDDER, null, "Embeds")));
		SecurityCatalogDependencyUtils.addAutomaticDependencies(admin, "agent1", "WORKSPACE",
				List.of(new Dependency("model1", "MODEL", true, CatalogDependencyRole.AGENT_MODEL, null, null)));
		QueryExecutionUtility.executeUpdate(securityDb,
				"INSERT INTO CATALOGDEPENDENCIES (CATALOGID, CATALOGTYPE, DEPENDENCYID, DEPENDENCYTYPE, USERID, "
						+ "IS_AUTOMATIC, ROLE) VALUES (?, ?, ?, ?, ?, ?, ?)",
				ps -> {
					ps.setString(1, "agent1");
					ps.setString(2, "WORKSPACE");
					ps.setString(3, "gone");
					ps.setString(4, "SKILL");
					ps.setString(5, "adminid");
					ps.setBoolean(6, true);
					ps.setString(7, CatalogDependencyRole.SKILL.name());
				});
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Map<String, Object>> byId(Object rows) {
		Map<String, Map<String, Object>> byId = new LinkedHashMap<>();
		for (Object row : (List<?>) rows) {
			Map<String, Object> map = (Map<String, Object>) row;
			byId.put((String) map.get("id"), map);
		}
		return byId;
	}

	@Test
	@SuppressWarnings("unchecked")
	void theWholeGraphHasEveryItemAndLink() throws Exception {
		createGraph();
		Map<String, Object> graph = AdminGetCatalogDependencyGraphReactor.buildGraph(null, Direction.BOTH, null);

		Map<String, Map<String, Object>> nodes = byId(graph.get("nodes"));
		assertEquals(Set.of("model1", "vector1", "lonely", "agent1", "gone"), nodes.keySet());
		Map<String, Object> vector = nodes.get("vector1");
		assertEquals("ENGINE", vector.get("kind"));
		assertEquals("VECTOR", vector.get("type"));
		assertEquals("Vector", vector.get("name"));
		assertEquals("Some vectors", vector.get("description"));
		assertEquals(false, vector.get("missing"));
		assertEquals(1, vector.get("dependencyCount"));
		assertEquals(1, vector.get("dependentCount"));
		assertTrue(vector.get("dateCreated") instanceof String);
		assertEquals("adminid", vector.get("createdBy"));

		Map<String, Object> agent = nodes.get("agent1");
		assertEquals("PROJECT", agent.get("kind"));
		assertEquals("WORKSPACE", agent.get("type"));
		assertNull(agent.get("subtype"));
		assertEquals(3, agent.get("dependencyCount"));

		Map<String, Object> gone = nodes.get("gone");
		assertEquals(true, gone.get("missing"));
		assertEquals("PROJECT", gone.get("kind"));
		assertEquals("SKILL", gone.get("type"));
		assertEquals("gone", gone.get("name"));

		assertEquals(-1, nodes.get("lonely").get("component"));
		assertEquals(0, nodes.get("agent1").get("component"));

		Map<String, Map<String, Object>> edges = byId(graph.get("edges"));
		assertEquals(Set.of("agent1|vector1|MANUAL", "vector1|model1|EMBEDDER", "agent1|model1|AGENT_MODEL",
				"agent1|gone|SKILL"), edges.keySet());
		Map<String, Object> embedder = edges.get("vector1|model1|EMBEDDER");
		assertEquals("vector1", embedder.get("source"));
		assertEquals("model1", embedder.get("target"));
		assertEquals(true, embedder.get("isAutomatic"));
		assertEquals("Embeds", embedder.get("description"));
		assertEquals(false, embedder.get("inCycle"));
		assertEquals("adminid", embedder.get("addedBy"));
		assertEquals(false, edges.get("agent1|vector1|MANUAL").get("isAutomatic"));

		Map<String, Object> stats = (Map<String, Object>) graph.get("stats");
		assertEquals(5, stats.get("nodes"));
		assertEquals(4, stats.get("edges"));
		assertEquals(3, stats.get("automatic"));
		assertEquals(1, stats.get("manual"));
		assertEquals(1, stats.get("missing"));
		assertEquals(1, stats.get("isolated"));
		assertEquals(0, stats.get("cycles"));
		assertEquals(1, stats.get("components"));
		assertEquals(Map.of("width", 240, "height", 64), graph.get("nodeSize"));
		Map<String, Object> bounds = (Map<String, Object>) graph.get("bounds");
		assertTrue(((Number) bounds.get("width")).doubleValue() >= 240);
	}

	@Test
	void aTracedGraphHasOnlyWhatTheItemReaches() throws Exception {
		createGraph();
		Map<String, Object> dependents = AdminGetCatalogDependencyGraphReactor.buildGraph("model1",
				Direction.DEPENDENTS, 1);
		assertEquals(Set.of("model1", "vector1", "agent1"), byId(dependents.get("nodes")).keySet());
		assertEquals(Set.of("vector1|model1|EMBEDDER", "agent1|model1|AGENT_MODEL", "agent1|vector1|MANUAL"),
				byId(dependents.get("edges")).keySet());

		Map<String, Object> dependencies = AdminGetCatalogDependencyGraphReactor.buildGraph("vector1",
				Direction.DEPENDENCIES, null);
		assertEquals(Set.of("vector1", "model1"), byId(dependencies.get("nodes")).keySet());

		assertThrows(IllegalArgumentException.class,
				() -> AdminGetCatalogDependencyGraphReactor.buildGraph("nothing", Direction.BOTH, null));
	}

	///
	/// getUserCatalogAccess
	///

	@Test
	void effectiveAccessTakesTheBestGrantAndNamesItsSource() throws Exception {
		User member = UnitTestSecurityAuthUtils.createUser("member", false);
		AdminSecurityGroupUtils groups = AdminSecurityGroupUtils.getInstance(admin);
		groups.addGroup(admin, "team", "CUSTOM", "catalog access team");
		groups.addUserToGroup(admin, "team", "memberid", AuthProvider.NATIVE.getLabel(), null);

		// direct edit
		UnitTestSecurityAuthUtils.createEngine("direct", "Direct", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "memberid", "direct", "EDIT");
		// team owner beats direct read only
		UnitTestSecurityAuthUtils.createEngine("team", "Team", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "memberid", "team", "READ_ONLY");
		groups.addGroupEnginePermission(admin, "team", "CUSTOM", "team", 1, null);
		// direct wins a tie with the team
		UnitTestSecurityAuthUtils.createEngine("tie", "Tie", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "memberid", "tie", "EDIT");
		groups.addGroupEnginePermission(admin, "team", "CUSTOM", "tie", 2, null);
		// public, nothing else
		UnitTestSecurityAuthUtils.createEngineGlobal("public", "Public", admin);
		// public with a direct read only: direct wins the tie
		UnitTestSecurityAuthUtils.createEngineGlobal("publicdirect", "Public Direct", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "memberid", "publicdirect", "READ_ONLY");
		// no access
		UnitTestSecurityAuthUtils.createEngine("hidden", "Hidden", admin);
		// a project through the team
		UnitTestSecurityAuthUtils.createProject("teamproject", "Team Project", admin);
		groups.addGroupProjectPermission(admin, "team", "CUSTOM", "teamproject", 3, null);

		Map<String, Map<String, Object>> access = byId(SecurityAdminUtils.getUserCatalogAccess("memberid"));
		assertEquals(Set.of("direct", "team", "tie", "public", "publicdirect", "teamproject"), access.keySet());
		assertAccess(access.get("direct"), "ENGINE", 2, "DIRECT");
		assertAccess(access.get("team"), "ENGINE", 1, "TEAM");
		assertAccess(access.get("tie"), "ENGINE", 2, "DIRECT");
		assertAccess(access.get("public"), "ENGINE", 3, "PUBLIC");
		assertAccess(access.get("publicdirect"), "ENGINE", 3, "DIRECT");
		assertAccess(access.get("teamproject"), "PROJECT", 3, "TEAM");
		assertFalse(access.containsKey("hidden"));

		// the owner of everything
		Map<String, Map<String, Object>> adminAccess = byId(SecurityAdminUtils.getUserCatalogAccess("adminid"));
		assertAccess(adminAccess.get("hidden"), "ENGINE", 1, "DIRECT");
		assertTrue(member != null);
	}

	@Test
	void effectiveAccessRules() {
		assertNull(SecurityAdminUtils.effectiveAccess(null, null, false));
		assertEquals(Map.entry(3, SecurityAdminUtils.CatalogAccessSource.PUBLIC),
				SecurityAdminUtils.effectiveAccess(null, null, true));
		assertEquals(Map.entry(2, SecurityAdminUtils.CatalogAccessSource.TEAM),
				SecurityAdminUtils.effectiveAccess(3, 2, true));
		assertEquals(Map.entry(1, SecurityAdminUtils.CatalogAccessSource.DIRECT),
				SecurityAdminUtils.effectiveAccess(1, 1, true));
		assertEquals(Map.entry(3, SecurityAdminUtils.CatalogAccessSource.TEAM),
				SecurityAdminUtils.effectiveAccess(null, 3, true));
		assertThrows(IllegalArgumentException.class, () -> SecurityAdminUtils.getUserCatalogAccess(" "));
	}

	private static void assertAccess(Map<String, Object> row, String kind, int permission, String source) {
		assertEquals(kind, row.get("kind"));
		assertEquals(permission, row.get("permission"));
		assertEquals(source, row.get("source"));
	}
}
