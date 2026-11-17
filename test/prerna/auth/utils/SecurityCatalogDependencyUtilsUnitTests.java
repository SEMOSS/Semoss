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
		SecurityCatalogDependencyUtils.setDependencies(owner, "function1", "FUNCTION",
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
		List<String> failed = SecurityCatalogDependencyUtils.setDependencyIds(owner, "automation1", "AUTOMATION",
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
		SecurityCatalogDependencyUtils.setDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("model1", "MODEL")));
		SecurityCatalogDependencyUtils.setDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("database1", "DATABASE"), new Dependency("database1", "DATABASE"),
						new Dependency("function1", "FUNCTION")));

		assertEquals(List.of("database1"), ids(SecurityCatalogDependencyUtils.getDependencies("function1")));
	}

	@Test
	void addDependencies_keepsTheOnesThere() {
		SecurityCatalogDependencyUtils.setDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("model1", "MODEL")));
		SecurityCatalogDependencyUtils.addDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("model1", "MODEL"), new Dependency("skill1", "SKILL")));

		assertEquals(2, SecurityCatalogDependencyUtils.getDependencies("function1").size());
	}

	@Test
	void getDependencies_followsEnginesAndProjectsAndEndsOnACycle() {
		SecurityCatalogDependencyUtils.setDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("skill1", "SKILL")));
		SecurityCatalogDependencyUtils.setDependencies(owner, "skill1", "SKILL",
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
		SecurityCatalogDependencyUtils.setDependencies(owner, "function1", "FUNCTION",
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
		SecurityCatalogDependencyUtils.setDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("model1", "MODEL")));
		SecurityCatalogDependencyUtils.setDependencies(owner, "skill1", "SKILL",
				List.of(new Dependency("function1", "FUNCTION")));

		SecurityEngineUtils.deleteEngine("function1");

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
		SecurityCatalogDependencyUtils.setDependencies(owner, "function1", "FUNCTION",
				List.of(new Dependency("notebook1", "NOTEBOOK")));

		SecurityProjectUtils.updateProject("notebook1", "Notebook", "CODE", null, false);

		assertEquals("CODE", SecurityCatalogDependencyUtils.getDependencies("function1").get(0).get("catalog_type"));
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
