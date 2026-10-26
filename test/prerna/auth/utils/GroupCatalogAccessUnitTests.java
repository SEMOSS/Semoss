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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.project.MyProjectsReactor;
import prerna.reactor.project.ProjectInfoReactor;
import prerna.reactor.security.EngineInfoReactor;
import prerna.reactor.security.GetEngineMetaValuesReactor;
import prerna.reactor.security.GetProjectMetaValuesReactor;
import prerna.reactor.security.MyEnginesReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.DIHelper;
import prerna.util.SocialPropertiesUtil;
import prerna.util.SystemEngineRegistry;

/** Runs the public catalog reactors against a disposable security database. */
class GroupCatalogAccessUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;
	private User admin;
	private User member;
	private AdminSecurityGroupUtils groups;
	private List<String> tables = new ArrayList<>();
	private final Set<String> loadedProjects = new HashSet<>();

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		member = UnitTestSecurityAuthUtils.createUser("member", false);
		groups = AdminSecurityGroupUtils.getInstance(admin);
	}

	@AfterEach
	void cleanup() throws Exception {
		for (String projectId : loadedProjects) {
			DIHelper.getInstance().removeProjectProperty(projectId);
		}
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	static Stream<Arguments> groupAccessCases() {
		return Stream.of("engine", "project").flatMap(kind -> Stream.of("MICROSOFT", "GOOGLE", "CUSTOM")
				.flatMap(type -> Stream.of(1, 2, 3).map(permission -> Arguments.of(kind, type, permission))));
	}

	static Stream<Arguments> membershipCases() {
		return Stream.of("engine", "project")
				.flatMap(kind -> Stream.of("MICROSOFT", "CUSTOM").map(type -> Arguments.of(kind, type)));
	}

	@ParameterizedTest
	@MethodSource("groupAccessCases")
	void groupOnlyAccessAppearsInBothListAndInfo(String kind, String type, int permission) throws Exception {
		joinGroup(type, "group");
		createResource(kind, "shared");
		createResource(kind, "unshared");
		grant(kind, "shared", "group", type, permission);
		assertNoDirectAccess(kind, "shared");

		List<Map<String, Object>> rows = list(kind, Map.of());
		assertEquals(1, rows.size());
		assertEquals("shared", rows.getFirst().get(kind + "_id"));
		assertPermission(kind, rows.getFirst(), permission, null);
		assertEquals(List.of(AccessPermissionEnum.getPermissionValueById(permission)),
				groupPermissions(kind, "shared"));
		assertEquals("shared", info(kind, "shared").get(kind + "_id"));
		assertThrows(IllegalArgumentException.class, () -> info(kind, "unshared"));
	}

	@ParameterizedTest
	@CsvSource({ "engine, READ_ONLY", "project, READ_ONLY", "engine, OWNER", "project, OWNER" })
	void overlappingGroupsAndDirectAccessReturnOneRowWithTheBestPermission(String kind, String directPermission)
			throws Exception {
		joinGroup("MICROSOFT", "readers");
		joinGroup("MICROSOFT", "editors");
		createResource(kind, "shared");
		grant(kind, "shared", "readers", "MICROSOFT", 3);
		grant(kind, "shared", "editors", "MICROSOFT", 2);
		if (kind.equals("engine")) {
			UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "memberid", "shared", directPermission);
		} else {
			UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, "shared", "memberid", directPermission);
		}
		List<Map<String, Object>> rows = list(kind, Map.of());
		assertEquals(1, rows.size());
		assertPermission(kind, rows.getFirst(), 2, AccessPermissionEnum.getIdByPermission(directPermission));
		assertEquals("shared", info(kind, "shared").get(kind + "_id"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "engine", "project" })
	void filtersAndPaginationKeepGroupOnlyRows(String kind) throws Exception {
		joinGroup("MICROSOFT", "group");
		for (String id : List.of("alpha", "beta", "gamma")) {
			createResource(kind, id);
			grant(kind, id, "group", "MICROSOFT", id.equals("gamma") ? 3 : 2);
		}
		assertEquals(List.of("alpha", "beta"), ids(kind, list(kind, Map.of("effectivePermissions", 2))));
		assertEquals(List.of("beta"), ids(kind, list(kind, Map.of("filterWord", "beta"))));
		assertEquals(List.of("gamma"), ids(kind, list(kind, Map.of(kind, "gamma"))));
		assertEquals(List.of("beta"), ids(kind, list(kind, Map.of("limit", "1", "offset", "1"))));
		assertTrue(list(kind, Map.of("onlyFavorites", true)).isEmpty());
	}

	@ParameterizedTest
	@MethodSource("membershipCases")
	void resourceIdQueriesIncludeGroupOnlyAccess(String kind, String type) throws Exception {
		joinGroup(type, "group");
		createResource(kind, "shared");
		createResource(kind, "unshared");
		grant(kind, "shared", "group", type, 3);
		assertEquals(List.of("shared"), resourceIds(kind, false, true));
	}

	@ParameterizedTest
	@MethodSource("membershipCases")
	void expiredGroupGrantsAreExcludedBeforeOpeningTheDetails(String kind, String type) throws Exception {
		joinGroup(type, "group");
		createResource(kind, "shared");
		grant(kind, "shared", "group", type, 3);
		expire("GROUP" + kind.toUpperCase() + "PERMISSION");
		assertTrue(list(kind, Map.of()).isEmpty());
		assertTrue(resourceIds(kind, false, true).isEmpty());
		assertTrue(details(kind, "shared").isEmpty());
		assertTrue(groupPermissions(kind, "shared").isEmpty());
		assertThrows(IllegalArgumentException.class, () -> info(kind, "shared"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "engine", "project" })
	void expiredCustomMembershipDoesNotProvideCatalogAccess(String kind) throws Exception {
		joinGroup("CUSTOM", "group");
		createResource(kind, "shared");
		grant(kind, "shared", "group", "CUSTOM", 3);
		expire("CUSTOMGROUPASSIGNMENT");
		assertTrue(list(kind, Map.of()).isEmpty());
		assertTrue(resourceIds(kind, false, true).isEmpty());
		assertTrue(details(kind, "shared").isEmpty());
		assertTrue(groupPermissions(kind, "shared").isEmpty());
		assertThrows(IllegalArgumentException.class, () -> info(kind, "shared"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "engine", "project" })
	void groupIdentityIncludesItsProviderInBothQueryOverloads(String kind) throws Exception {
		joinGroup("MICROSOFT", "group");
		groups.addGroup(admin, "group", "GOOGLE", "separate provider");
		createResource(kind, "unshared");
		grant(kind, "unshared", "group", "GOOGLE", 3);
		assertTrue(list(kind, Map.of()).isEmpty());
		assertThrows(IllegalArgumentException.class, () -> info(kind, "unshared"));
		assertTrue(details(kind, "unshared").isEmpty());
		assertTrue(resourceIds(kind, false, true).isEmpty());
		assertTrue(groupPermissions(kind, "unshared").isEmpty());
	}

	@ParameterizedTest
	@MethodSource("membershipCases")
	void futureGroupGrantsRemainAvailable(String kind, String type) throws Exception {
		joinGroup(type, "group");
		createResource(kind, "shared");
		grant(kind, "shared", "group", type, 2);
		setEndDate("GROUP" + kind.toUpperCase() + "PERMISSION", Instant.now().plus(1, ChronoUnit.DAYS));
		if (type.equals("CUSTOM")) {
			setEndDate("CUSTOMGROUPASSIGNMENT", Instant.now().plus(1, ChronoUnit.DAYS));
		}
		assertEquals(List.of("shared"), ids(kind, list(kind, Map.of())));
		assertEquals(List.of("shared"), resourceIds(kind, false, true));
		assertEquals("shared", info(kind, "shared").get(kind + "_id"));
	}

	@ParameterizedTest
	@MethodSource("membershipCases")
	void excludingExistingAccessAlsoExcludesGroupAccess(String kind, String type) throws Exception {
		joinGroup(type, "group");
		for (String id : List.of("shared", "discoverable", "private")) {
			createResource(kind, id);
		}
		grant(kind, "shared", "group", type, 3);
		setDiscoverable(kind, "shared", true);
		setDiscoverable(kind, "discoverable", true);
		assertEquals(List.of("discoverable"), resourceIds(kind, true, false));
	}

	@ParameterizedTest
	@ValueSource(strings = { "engine", "project" })
	void usersWithoutGroupsOnlySeeResourcesTheyCanAccess(String kind) throws Exception {
		createResource(kind, "private");
		createResource(kind, "discoverable");
		createResource(kind, "direct");
		setDiscoverable(kind, "discoverable", true);
		if (kind.equals("engine")) {
			UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "memberid", "direct", "READ_ONLY");
		} else {
			UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, "direct", "memberid", "READ_ONLY");
		}
		assertEquals(List.of("direct"), ids(kind, list(kind, Map.of())));
		assertEquals(List.of("direct"), resourceIds(kind, false, true));
		assertEquals(List.of("discoverable"), resourceIds(kind, true, false));
		assertEquals("direct", info(kind, "direct").get(kind + "_id"));
		assertEquals("discoverable", info(kind, "discoverable").get(kind + "_id"));
		assertThrows(IllegalArgumentException.class, () -> info(kind, "private"));
	}

	@ParameterizedTest
	@CsvSource({ "engine, true", "project, true", "engine, false", "project, false" })
	void groupAccessSupportsFavoritesAndVisibilityWithoutADirectGrant(String kind, boolean favoriteFirst)
			throws Exception {
		joinGroup("MICROSOFT", "group");
		createResource(kind, "shared");
		grant(kind, "shared", "group", "MICROSOFT", 3);
		if (!favoriteFirst) {
			setVisibility(kind, "shared", true);
			assertNoDirectAccess(kind, "shared");
		}
		if (kind.equals("engine")) {
			SecurityEngineUtils.setEngineFavorite(member, "shared", true);
		} else {
			SecurityProjectUtils.setProjectFavorite(member, "shared", true);
		}
		assertNoDirectAccess(kind, "shared");
		assertEquals(List.of("shared"), ids(kind, list(kind, Map.of("onlyFavorites", true))));
		setVisibility(kind, "shared", false);
		assertTrue(list(kind, Map.of()).isEmpty());
		assertEquals("shared", info(kind, "shared").get(kind + "_id"));
		assertEquals(List.of("shared"), resourceIds(kind, false, true));
		setVisibility(kind, "shared", true);
		assertPermission(kind, list(kind, Map.of()).getFirst(), 3, null);
		expire("GROUP" + kind.toUpperCase() + "PERMISSION");
		assertTrue(list(kind, Map.of()).isEmpty());
		assertTrue(resourceIds(kind, false, true).isEmpty());
		assertTrue(details(kind, "shared").isEmpty());
		assertThrows(IllegalArgumentException.class, () -> info(kind, "shared"));
	}

	@ParameterizedTest
	@MethodSource("membershipCases")
	@SuppressWarnings("unchecked")
	void metadataQueriesIncludeOnlyAccessibleGroupRows(String kind, String type) throws Exception {
		joinGroup(type, "group");
		for (String id : List.of("shared", "unshared")) {
			createResource(kind, id);
			if (kind.equals("engine")) {
				SecurityEngineUtils.updateEngineMetadata(id, Map.of("tag", id));
			} else {
				SecurityProjectUtils.updateProjectMetadata(id, Map.of("tag", id));
			}
		}
		grant(kind, "shared", "group", type, 3);
		List<Map<String, Object>> filtered = list(kind, Map.of("metaFilters", Map.of("tag", "shared")));
		assertEquals(List.of("shared"), ids(kind, filtered));
		assertEquals("shared", filtered.getFirst().get("tag"));
		assertTrue(list(kind, Map.of("metaFilters", Map.of("tag", "unshared"))).isEmpty());
		AbstractReactor reactor = kind.equals("engine") ? new GetEngineMetaValuesReactor()
				: new GetProjectMetaValuesReactor();
		configure(reactor, Map.of("metaKeys", "tag"));
		List<Map<String, Object>> values = (List<Map<String, Object>>) reactor.execute().getValue();
		assertEquals(1, values.size());
		assertEquals("shared", values.getFirst().get("METAVALUE"));
		assertEquals(1, ((Number) values.getFirst().get("count")).intValue());
	}

	private void joinGroup(String type, String groupId) throws Exception {
		groups.addGroup(admin, groupId, type, "catalog test group");
		if (type.equals("CUSTOM")) {
			groups.addUserToGroup(admin, groupId, "memberid", AuthProvider.NATIVE.getLabel(), null);
		} else {
			AuthProvider provider = AuthProvider.getProviderFromString(type);
			AccessToken token = member.getAccessToken(provider) == null ? new AccessToken()
					: AccessToken.copyToken(member.getAccessToken(provider));
			token.setId("memberid");
			token.setProvider(provider);
			token.setUserGroupType(type);
			Set<String> memberships = new HashSet<>(token.getUserGroups());
			memberships.add(groupId);
			token.setUserGroups(memberships);
			member.setAccessToken(token);
		}
	}

	private void createResource(String kind, String id) throws Exception {
		if (kind.equals("engine")) {
			UnitTestSecurityAuthUtils.createEngine(id, id, admin);
			SecurityEngineUtils.setEngineDiscoverable(admin, id, false);
		} else {
			UnitTestSecurityAuthUtils.createProject(id, id, admin);
			SecurityProjectUtils.setProjectDiscoverable(admin, id, false);
			IProject project = mock(IProject.class);
			when(project.getSmssProp()).thenReturn(new Properties());
			when(project.getEngineId()).thenReturn(id);
			when(project.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.PROJECT);
			DIHelper.getInstance().setProjectProperty(id, project);
			loadedProjects.add(id);
		}
	}

	private void grant(String kind, String id, String groupId, String type, int permission) {
		if (kind.equals("engine")) {
			groups.addGroupEnginePermission(admin, groupId, type, id, permission, null);
		} else {
			groups.addGroupProjectPermission(admin, groupId, type, id, permission, null);
		}
	}

	private void assertNoDirectAccess(String kind, String id) {
		assertFalse(kind.equals("engine") ? SecurityUserEngineUtils.userCanViewEngine(member, id)
				: SecurityUserProjectUtils.userCanViewProject(member, id));
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> list(String kind, Map<String, Object> arguments) {
		AbstractReactor reactor = kind.equals("engine") ? new MyEnginesReactor() : new MyProjectsReactor();
		configure(reactor, arguments);
		return (List<Map<String, Object>>) reactor.execute().getValue();
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> info(String kind, String id) {
		AbstractReactor reactor = kind.equals("engine") ? new EngineInfoReactor() : new ProjectInfoReactor();
		configure(reactor, Map.of(kind, id));
		// Only the deployment URL is stubbed; authorization and catalog queries use the
		// database.
		try (MockedStatic<SocialPropertiesUtil> social = mockStatic(SocialPropertiesUtil.class)) {
			SocialPropertiesUtil properties = mock(SocialPropertiesUtil.class);
			when(properties.getProperty("redirect")).thenReturn("http://localhost:8080/Monolith/oauth");
			social.when(SocialPropertiesUtil::getInstance).thenReturn(properties);
			return (Map<String, Object>) reactor.execute().getValue();
		}
	}

	private List<Map<String, Object>> details(String kind, String id) {
		return kind.equals("engine") ? SecurityEngineUtils.getUserEngineList(member, id, null)
				: SecurityProjectUtils.getUserProjectList(member, id);
	}

	private List<String> resourceIds(String kind, boolean includeDiscoverable, boolean includeExistingAccess) {
		return kind.equals("engine")
				? SecurityEngineUtils.getUserEngineIdList(member, null, false, includeDiscoverable,
						includeExistingAccess)
				: SecurityProjectUtils.getUserProjectIdList(member, null, false, includeDiscoverable,
						includeExistingAccess);
	}

	private List<String> groupPermissions(String kind, String id) {
		return kind.equals("engine") ? SecurityUserEngineUtils.getActualGroupUserEnginePermission(member, id)
				: SecurityUserProjectUtils.getActualGroupUserProjectPermission(member, id);
	}

	private void setDiscoverable(String kind, String id, boolean discoverable) throws Exception {
		if (kind.equals("engine")) {
			SecurityEngineUtils.setEngineDiscoverable(admin, id, discoverable);
		} else {
			SecurityProjectUtils.setProjectDiscoverable(admin, id, discoverable);
		}
	}

	private void setVisibility(String kind, String id, boolean visible) throws Exception {
		if (kind.equals("engine")) {
			SecurityEngineUtils.setEngineVisibility(member, id, visible);
		} else {
			SecurityProjectUtils.setProjectVisibility(member, id, visible);
		}
	}

	private void configure(AbstractReactor reactor, Map<String, Object> arguments) {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(member);
		reactor.setInsight(insight);
		NounStore nouns = new NounStore(reactor.getClass().getSimpleName());
		arguments.forEach((key, value) -> nouns.makeGenRowStruct(key).add(value,
				value instanceof Boolean ? PixelDataType.BOOLEAN
						: value instanceof Integer ? PixelDataType.CONST_INT
								: value instanceof Map ? PixelDataType.MAP : PixelDataType.CONST_STRING));
		reactor.setNounStore(nouns);
	}

	private List<Object> ids(String kind, List<Map<String, Object>> rows) {
		return rows.stream().map(row -> row.get(kind + "_id")).toList();
	}

	private void assertPermission(String kind, Map<String, Object> row, int groupPermission, Integer directPermission) {
		String prefix = kind.equals("engine") ? "engine_" : "";
		assertEquals(groupPermission, ((Number) row.get(prefix + "group_permission")).intValue());
		assertEquals(directPermission == null ? groupPermission : Math.min(groupPermission, directPermission),
				((Number) row.get("permission")).intValue());
		if (directPermission == null) {
			assertNull(row.get(prefix + "user_permission"));
		} else {
			assertEquals(directPermission.intValue(), ((Number) row.get(prefix + "user_permission")).intValue());
		}
	}

	private void expire(String table) throws Exception {
		setEndDate(table, Instant.now().minus(1, ChronoUnit.DAYS));
	}

	private void setEndDate(String table, Instant endDate) throws Exception {
		try (Connection connection = securityDb.getConnection();
				PreparedStatement statement = connection.prepareStatement("UPDATE " + table + " SET ENDDATE=?")) {
			statement.setTimestamp(1, Timestamp.from(endDate));
			statement.executeUpdate();
			if (!connection.getAutoCommit()) {
				connection.commit();
			}
		}
	}
}
