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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityCatalogDependencyUtils.Dependency;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Dependencies of engines and projects, each kept with its own engine type or
 * project type.
 */
public class SecurityCatalogDependencyUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();
	private User owner;

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		UnitTestSecurityAuthUtils.createEngine("function1", "Function", IEngine.CATALOG_TYPE.FUNCTION, owner);
		UnitTestSecurityAuthUtils.createEngine("model1", "Model", IEngine.CATALOG_TYPE.MODEL, owner);
		UnitTestSecurityAuthUtils.createEngine("database1", "Database", IEngine.CATALOG_TYPE.DATABASE, owner);
		UnitTestSecurityAuthUtils.createProjectOfType("skill1", "Skill", "SKILL", owner);
		UnitTestSecurityAuthUtils.createProjectOfType("notebook1", "Notebook", "NOTEBOOK", owner);
		UnitTestSecurityAuthUtils.createProjectOfType("automation1", "Automation", "AUTOMATION", owner);
	}

	@AfterEach
	void cleanup() throws SQLException {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void kindOf_tellsEngineTypesFromProjectTypes() {
		for (String type : List.of("FUNCTION", "MODEL", "DATABASE", "VECTOR", "STORAGE", "GUARDRAIL")) {
			assertEquals(CatalogTypes.Kind.ENGINE, CatalogTypes.kindOf(type), type);
		}
		for (String type : List.of("CODE", "BLOCKS", "INSIGHTS", "SKILL", "NOTEBOOK", "AUTOMATION", "WORKSPACE")) {
			assertEquals(CatalogTypes.Kind.PROJECT, CatalogTypes.kindOf(type), type);
		}
		assertEquals("INSIGHTS", CatalogTypes.normalize("insight"));
		assertThrows(IllegalArgumentException.class, () -> CatalogTypes.kindOf("PROJECT"));
		assertThrows(IllegalArgumentException.class, () -> CatalogTypes.kindOf("VENV"));
	}

	@Test
	void resolve_readsTheSpecificTypeWithoutLoadingAnything() {
		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class, Mockito.CALLS_REAL_METHODS)) {
			assertEquals("FUNCTION", CatalogTypes.resolve("function1"));
			assertEquals("SKILL", CatalogTypes.resolve("skill1"));
			assertNull(CatalogTypes.resolve("missing"));
			// on a cluster, loading would pull the engine or project onto this server
			utility.verify(() -> Utility.getEngine(Mockito.anyString()), Mockito.never());
			utility.verify(() -> Utility.getProject(Mockito.anyString()), Mockito.never());
		}
	}

	@Test
	void anEngineOwnsDependenciesOnEnginesAndProjects() {
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("model1", "MODEL"), new Dependency("skill1", "SKILL")));

		List<Map<String, Object>> details = SecurityCatalogDependencyUtils.getDependencyDetails("function1");
		Map<String, Map<String, Object>> byId = byCatalogId(details);
		assertEquals("MODEL", byId.get("model1").get("catalog_type"));
		assertEquals("Model", byId.get("model1").get("catalog_name"));
		assertEquals("SKILL", byId.get("skill1").get("catalog_type"));
		assertEquals("Skill", byId.get("skill1").get("catalog_name"));
		assertEquals("FUNCTION", queryString("SELECT DISTINCT CATALOGTYPE FROM CATALOGDEPENDENCIES"));
	}

	@Test
	void aProjectOwnsDependenciesOnEnginesAndProjects() {
		List<String> failed = SecurityCatalogDependencyUtils.syncAutomaticDependencyIds(owner, "automation1",
				"AUTOMATION", CatalogDependencyRole.AUTOMATION_NODE, "Used by the automation",
				List.of("database1", "notebook1", "missing"));

		assertEquals(List.of("missing"), failed);
		Map<String, Map<String, Object>> byId = byCatalogId(
				SecurityCatalogDependencyUtils.getDependencies("automation1"));
		assertEquals("DATABASE", byId.get("database1").get("catalog_type"));
		assertEquals("NOTEBOOK", byId.get("notebook1").get("catalog_type"));
		assertEquals(2, byId.size());
	}

	@Test
	void setDependencies_replacesAndSkipsTheOwnerAndRepeats() {
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("model1", "MODEL")));
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("database1", "DATABASE"), new Dependency("database1", "DATABASE"),
						new Dependency("function1", "FUNCTION")));

		assertEquals(List.of("database1"), ids(SecurityCatalogDependencyUtils.getDependencies("function1")));
	}

	@Test
	void addAutomaticDependencies_keepsTheOnesThere() {
		SecurityCatalogDependencyUtils.addAutomaticDependencies(owner, "function1", "FUNCTION",
				List.of(automatic("model1", "MODEL", CatalogDependencyRole.TOOLBOX)));
		SecurityCatalogDependencyUtils.addAutomaticDependencies(owner, "function1", "FUNCTION",
				List.of(automatic("model1", "MODEL", CatalogDependencyRole.TOOLBOX),
						automatic("skill1", "SKILL", CatalogDependencyRole.SKILL)));

		assertEquals(2, SecurityCatalogDependencyUtils.getDependencyDetails("function1").size());
	}

	@Test
	void manualAndAutomaticDependenciesAreSavedApart() {
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "automation1", "AUTOMATION",
				List.of(Dependency.manual("database1", "DATABASE", "salesDb", "Sales numbers")));
		SecurityCatalogDependencyUtils.syncAutomaticDependencies(owner, "automation1", "AUTOMATION",
				List.of(CatalogDependencyRole.AUTOMATION_NODE),
				List.of(automatic("model1", "MODEL", CatalogDependencyRole.AUTOMATION_NODE)));

		// saving the automation again keeps the picked one
		SecurityCatalogDependencyUtils.syncAutomaticDependencies(owner, "automation1", "AUTOMATION",
				List.of(CatalogDependencyRole.AUTOMATION_NODE),
				List.of(automatic("function1", "FUNCTION", CatalogDependencyRole.AUTOMATION_NODE)));
		Map<String, Map<String, Object>> byId = byCatalogId(
				SecurityCatalogDependencyUtils.getDependencyDetails("automation1"));
		assertEquals(java.util.Set.of("database1", "function1"), byId.keySet());
		assertEquals(false, byId.get("database1").get("is_automatic"));
		assertEquals("MANUAL", byId.get("database1").get("role"));
		assertEquals("salesDb", byId.get("database1").get("dependency_name"));
		assertEquals("Sales numbers", byId.get("database1").get("dependency_description"));
		assertEquals(true, byId.get("function1").get("is_automatic"));
		assertEquals("AUTOMATION_NODE", byId.get("function1").get("role"));
		assertNull(byId.get("function1").get("dependency_name"));

		// and setting the picked ones keeps the automatic one
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "automation1", "AUTOMATION", List.of());
		assertEquals(List.of("function1"), ids(SecurityCatalogDependencyUtils.getDependencies("automation1")));
	}

	private static Map<String, Object> dependencyNode(Integer permission, Integer groupPermission, Boolean global,
			Boolean discoverable) {
		Map<String, Object> node = new HashMap<>();
		node.put("catalog_id", "db1");
		node.put("catalog_type", "DATABASE");
		node.put("catalog_name", "Payroll");
		node.put("description", "Salaries");
		node.put("tags", "hr");
		node.put("catalog_subtype", "H2_DB");
		node.put("catalog_date_created", "2026-01-01");
		node.put("permission", permission);
		node.put("group_permission", groupPermission);
		node.put("catalog_global", global);
		node.put("catalog_discoverable", discoverable);
		return node;
	}

	@Test
	void redactPrivateDependency_removesWhatAPrivateItemIs() {
		Map<String, Object> node = SecurityCatalogDependencyUtils
				.redactPrivateDependency(dependencyNode(null, null, false, false));
		assertEquals(Boolean.TRUE, node.get("is_private"));
		assertEquals("db1", node.get("catalog_id"));
		assertEquals("DATABASE", node.get("catalog_type"));
		for (String key : List.of("catalog_name", "description", "tags", "catalog_subtype",
				"catalog_date_created")) {
			assertFalse(node.containsKey(key), key);
		}
	}

	@Test
	void redactPrivateDependency_keepsItemsTheUserCanSee() {
		List<Map<String, Object>> visible = List.of(dependencyNode(null, null, false, true),
				dependencyNode(null, null, true, false), dependencyNode(3, null, false, false),
				dependencyNode(null, 2, false, false));
		for (Map<String, Object> node : visible) {
			SecurityCatalogDependencyUtils.redactPrivateDependency(node);
			assertEquals(Boolean.FALSE, node.get("is_private"));
			assertEquals("Payroll", node.get("catalog_name"));
			assertEquals("Salaries", node.get("description"));
		}
	}

	@Test
	void syncAutomaticDependencies_replacesOnlyItsOwnRoles() {
		UnitTestSecurityAuthUtils.createEngine("guardrail1", "Guardrail", IEngine.CATALOG_TYPE.GUARDRAIL, owner);
		SecurityCatalogDependencyUtils.syncAutomaticDependencies(owner, "model1", "MODEL",
				List.of(CatalogDependencyRole.GUARDRAIL),
				List.of(automatic("guardrail1", "GUARDRAIL", CatalogDependencyRole.GUARDRAIL)));
		SecurityCatalogDependencyUtils.syncAutomaticDependencies(owner, "model1", "MODEL",
				CatalogDependencyDerivation.ROUTER_ROLES,
				List.of(automatic("function1", "FUNCTION", CatalogDependencyRole.ROUTE_MODEL)));

		assertEquals(java.util.Set.of("guardrail1", "function1"),
				new java.util.HashSet<>(ids(SecurityCatalogDependencyUtils.getDependencies("model1"))));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityCatalogDependencyUtils.syncAutomaticDependencies(owner, "model1", "MODEL",
						List.of(CatalogDependencyRole.GUARDRAIL),
						List.of(automatic("function1", "FUNCTION", CatalogDependencyRole.ROUTE_MODEL))));
	}

	@Test
	void aDependencyWithTwoRolesIsTwoLinksAndOneDependency() {
		SecurityCatalogDependencyUtils.syncAutomaticDependencies(owner, "model1", "MODEL",
				CatalogDependencyDerivation.ROUTER_ROLES,
				List.of(automatic("function1", "FUNCTION", CatalogDependencyRole.ROUTE_MODEL),
						automatic("function1", "FUNCTION", CatalogDependencyRole.ROUTER_FALLBACK)));

		assertEquals(2, SecurityCatalogDependencyUtils.getDependencyDetails("model1").size());
		assertEquals(List.of("function1"), ids(SecurityCatalogDependencyUtils.getDependencies("model1")));
	}

	@Test
	void anAutomaticDependencyReplacesARowFromBeforeTheColumn() throws Exception {
		prerna.util.QueryExecutionUtility.executeUpdate(securityDb,
				"INSERT INTO CATALOGDEPENDENCIES (CATALOGID, CATALOGTYPE, DEPENDENCYID, DEPENDENCYTYPE) "
						+ "VALUES ('automation1', 'AUTOMATION', 'model1', 'MODEL')",
				prerna.util.QueryExecutionUtility.NO_PARAMETERS);
		Map<String, Object> legacy = SecurityCatalogDependencyUtils.getDependencyDetails("automation1").get(0);
		assertEquals(false, legacy.get("is_automatic"));
		assertEquals("MANUAL", legacy.get("role"));

		SecurityCatalogDependencyUtils.syncAutomaticDependencies(owner, "automation1", "AUTOMATION",
				List.of(CatalogDependencyRole.AUTOMATION_NODE),
				List.of(automatic("model1", "MODEL", CatalogDependencyRole.AUTOMATION_NODE)));

		List<Map<String, Object>> rows = SecurityCatalogDependencyUtils.getDependencyDetails("automation1");
		assertEquals(1, rows.size());
		assertEquals(true, rows.get(0).get("is_automatic"));
	}

	@Test
	void setManualDependencies_rejectsTwoWithOneName() {
		assertThrows(IllegalArgumentException.class,
				() -> SecurityCatalogDependencyUtils.setManualDependencies(owner, "automation1", "AUTOMATION",
						List.of(Dependency.manual("model1", "MODEL", "llm", null),
								Dependency.manual("database1", "DATABASE", "LLM", null))));
		assertTrue(SecurityCatalogDependencyUtils.getDependencies("automation1").isEmpty());
	}

	@Test
	void removeAutomaticDependency_keepsAPickedOneOnTheSameItem() {
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "automation1", "AUTOMATION",
				List.of(new Dependency("skill1", "SKILL")));
		SecurityCatalogDependencyUtils.addAutomaticDependencies(owner, "automation1", "AUTOMATION",
				List.of(automatic("skill1", "SKILL", CatalogDependencyRole.SKILL)));

		SecurityCatalogDependencyUtils.removeAutomaticDependency("automation1", "skill1", CatalogDependencyRole.SKILL);

		List<Map<String, Object>> rows = SecurityCatalogDependencyUtils.getDependencyDetails("automation1");
		assertEquals(1, rows.size());
		assertEquals("MANUAL", rows.get(0).get("role"));
	}

	@Test
	void getDependents_listsWhoDependsOnAnItem() {
		SecurityCatalogDependencyUtils.syncAutomaticDependencies(owner, "automation1", "AUTOMATION",
				List.of(CatalogDependencyRole.AUTOMATION_NODE),
				List.of(automatic("function1", "FUNCTION", CatalogDependencyRole.AUTOMATION_NODE)));
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("model1", "MODEL")));

		List<Map<String, Object>> direct = SecurityCatalogDependencyUtils.getDependents("model1", false);
		assertEquals(List.of("function1"), ids(direct));
		List<Map<String, Object>> all = SecurityCatalogDependencyUtils.getDependents("model1", true);
		assertEquals(List.of("function1", "automation1"), ids(all));
		assertEquals("AUTOMATION_NODE", all.get(1).get("role"));
		assertEquals("Automation", all.get(1).get("catalog_name"));
	}

	@Test
	void exportsKeepTheLink() {
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "automation1", "AUTOMATION",
				List.of(Dependency.manual("database1", "DATABASE", "salesDb", "Sales numbers")));
		SecurityCatalogDependencyUtils.addAutomaticDependencies(owner, "automation1", "AUTOMATION",
				List.of(automatic("model1", "MODEL", CatalogDependencyRole.AUTOMATION_NODE)));

		List<Dependency> exported = SecurityCatalogDependencyUtils
				.fromExported(SecurityCatalogDependencyUtils.getDependencyDetails("automation1"));

		assertTrue(exported.contains(Dependency.manual("database1", "DATABASE", "salesDb", "Sales numbers")));
		assertTrue(exported.contains(automatic("model1", "MODEL", CatalogDependencyRole.AUTOMATION_NODE)));
	}

	@Test
	void aCloneGetsTheSourcesLinksAsTheyAre() {
		// as EngineCloneUtility copies them: every link, automatic and picked
		UnitTestSecurityAuthUtils.createEngine("guardrail1", "Guardrail", IEngine.CATALOG_TYPE.GUARDRAIL, owner);
		UnitTestSecurityAuthUtils.createEngine("guardrail2", "Guardrail Copy", IEngine.CATALOG_TYPE.GUARDRAIL, owner);
		SecurityCatalogDependencyUtils.syncAutomaticDependencies(owner, "guardrail1", "GUARDRAIL",
				CatalogDependencyDerivation.SMSS_ROLES,
				List.of(automatic("model1", "MODEL", CatalogDependencyRole.JUDGE_MODEL)));
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "guardrail1", "GUARDRAIL",
				List.of(Dependency.manual("database1", "DATABASE", "policies", "Policy tables")));

		SecurityCatalogDependencyUtils.replaceAllDependencies(owner, "guardrail2", "GUARDRAIL",
				SecurityCatalogDependencyUtils
						.fromExported(SecurityCatalogDependencyUtils.getDependencyDetails("guardrail1")));

		Map<String, Map<String, Object>> byId = byCatalogId(
				SecurityCatalogDependencyUtils.getDependencyDetails("guardrail2"));
		assertEquals(java.util.Set.of("model1", "database1"), byId.keySet());
		assertEquals("JUDGE_MODEL", byId.get("model1").get("role"));
		assertEquals(true, byId.get("model1").get("is_automatic"));
		assertEquals("MANUAL", byId.get("database1").get("role"));
		assertEquals("policies", byId.get("database1").get("dependency_name"));
		assertEquals("Policy tables", byId.get("database1").get("dependency_description"));
		// the source keeps its own
		assertEquals(2, SecurityCatalogDependencyUtils.getDependencyDetails("guardrail1").size());
	}

	@Test
	void getDependencies_followsEnginesAndProjectsAndEndsOnACycle() {
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("skill1", "SKILL")));
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "skill1", "SKILL",
				List.of(new Dependency("model1", "MODEL"), new Dependency("function1", "FUNCTION")));

		List<Map<String, Object>> all = SecurityCatalogDependencyUtils.getDependencies("function1", true);

		// each item once: the skill, its model, and the function itself where the cycle
		// closes
		assertEquals(3, all.size());
		assertEquals(java.util.Set.of("skill1", "model1", "function1"), new java.util.HashSet<>(ids(all)));
	}

	@Test
	void validateDependencies_checksEachWithItsOwnPermissions() {
		User reader = UnitTestSecurityAuthUtils.createUser("reader", false);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(owner, "readerid", "model1", "READ_ONLY");
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("model1", "MODEL"), new Dependency("skill1", "SKILL")));

		Map<String, Object> canView = SecurityCatalogDependencyUtils.validateDependencies(reader, "function1");

		assertEquals(true, canView.get("model1"));
		assertEquals(false, canView.get("skill1"));
	}

	@Test
	void permissionsBranchOnTheOwnersType() {
		User other = UnitTestSecurityAuthUtils.createUser("other", false);
		assertTrue(SecurityCatalogDependencyUtils.userCanEdit(owner, "function1", "FUNCTION"));
		assertTrue(SecurityCatalogDependencyUtils.userCanEdit(owner, "skill1", "SKILL"));
		assertFalse(SecurityCatalogDependencyUtils.userCanEdit(other, "function1", "FUNCTION"));
		assertFalse(SecurityCatalogDependencyUtils.userCanView(other, "skill1", "SKILL"));
	}

	@Test
	void resolveCatalogType_checksAGivenTypeAgainstTheSecurityDatabase() {
		assertEquals("FUNCTION", SecurityCatalogDependencyUtils.resolveCatalogType("function1", null));
		assertEquals("SKILL", SecurityCatalogDependencyUtils.resolveCatalogType("skill1", "skill"));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityCatalogDependencyUtils.resolveCatalogType("skill1", "NOTEBOOK"));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityCatalogDependencyUtils.resolveCatalogType("skill1", "MODEL"));
		// a legacy project type, as the UI sends PROJECT.TYPE, is an INSIGHTS project
		UnitTestSecurityAuthUtils.createProject("legacyApp", "Legacy", owner);
		assertEquals("INSIGHTS", SecurityCatalogDependencyUtils.resolveCatalogType("legacyApp", "APP"));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityCatalogDependencyUtils.resolveCatalogType("missing", null));
	}

	@Test
	void deletingAnEngineRemovesTheDependenciesItOwned() {
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("model1", "MODEL")));
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "skill1", "SKILL",
				List.of(new Dependency("function1", "FUNCTION")));

		SecurityEngineUtils.deleteEngine(owner, "function1");

		assertTrue(SecurityCatalogDependencyUtils.getDependencies("function1").isEmpty());
		// the skill still lists it, so validating the skill shows it is gone
		assertEquals(List.of("function1"), ids(SecurityCatalogDependencyUtils.getDependencies("skill1")));
	}

	@Test
	void fromExported_readsOldAndNewExports() {
		List<Dependency> dependencies = SecurityCatalogDependencyUtils.fromExported(List.of(
				Map.of("engine_id", "skill1", "engine_type", "PROJECT"),
				Map.of("catalog_id", "model1", "catalog_type", "MODEL"),
				Map.of("engine_id", "platformTool", "engine_type", "FUNCTION"),
				Map.of("engine_id", "unknownProject", "engine_type", "PROJECT")));

		assertEquals(List.of(new Dependency("skill1", "SKILL"), new Dependency("model1", "MODEL"),
				new Dependency("platformTool", "FUNCTION")), dependencies);
	}

	@Test
	void changingAProjectsTypeUpdatesItsDependencyRows() {
		SecurityCatalogDependencyUtils.setManualDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("notebook1", "NOTEBOOK")));

		SecurityProjectUtils.updateProject("notebook1", "Notebook", "CODE", null, false);

		assertEquals("CODE", SecurityCatalogDependencyUtils.getDependencies("function1").get(0).get("catalog_type"));
	}

	private static Dependency automatic(String id, String type, CatalogDependencyRole role) {
		return Dependency.automatic(id, type, role, "Derived for the test");
	}

	private static Map<String, Map<String, Object>> byCatalogId(List<Map<String, Object>> rows) {
		Map<String, Map<String, Object>> byId = new HashMap<>();
		for (Map<String, Object> row : rows) {
			byId.put((String) row.get("catalog_id"), row);
		}
		return byId;
	}

	private static List<String> ids(List<Map<String, Object>> rows) {
		List<String> ids = new ArrayList<>();
		for (Map<String, Object> row : rows) {
			ids.add((String) row.get("catalog_id"));
		}
		return ids;
	}

	private String queryString(String sql) {
		try {
			return prerna.util.QueryExecutionUtility.queryOne(securityDb, sql,
					prerna.util.QueryExecutionUtility.NO_PARAMETERS, rs -> rs.getString(1));
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
