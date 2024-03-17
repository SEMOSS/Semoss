package prerna.testing.securitydb;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;

import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AdminSecurityGroupUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityNativeUserUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUserUtils;
import prerna.util.sql.RdbmsTypeEnum;

@TestMethodOrder(OrderAnnotation.class)
public class SecurityDbGroupTests extends AbstractBaseSemossApiTests {

	private static final String TEST_GROUP = "myNewGroup";
	private static final String TEST_GROUP_TYPE = null; // custom groups dont have a type
	private static final String TEST_GROUP_DESCRIPTION = "my description";
	private static final boolean TEST_GROUP_IS_CUSTOM = true;

	private static final String NATIVE_DUMMY_SEARCH = "dummy";
	private static final String NATIVE_DUMMY_USER_ID = "DUMMY_USER_123";
	private static final AuthProvider NATIVE_DUMMY_USER_PROVIDER = AuthProvider.NATIVE;
	private static final String NATIVE_DUMMY_NAME = "DUMMY NAME";
	private static final String NATIVE_DUMMY_USERNAME = "DUMMYUSERNAME";
	private static final String NATIVE_DUMMY_EMAIL = "example@mail.com";
	private static final String NATIVE_DUMMY_PASSWORD = "SEMoss@123123!@#";

	private static final String PERMISSION_TEST_PROJECTID = "testing-projectid";
	private static final String PERMISSION_TEST_APP_PROJECTID = "testing-projectid-app";
	private static final String PERMISSION_TEST_ENGINEID = "testing-engineid";

	
	@BeforeAll
    public static void initialSetup() throws Exception {
		AbstractBaseSemossApiTests.initialSetup();
		// unnecessary if running by itself, but necessary if running {@link prerna.testing.AllTests}
		ApiSemossTestEngineUtils.deleteAllDataAndAddUser();
	}
	
	@Override
	@BeforeEach
	public void beforeEachTest() {
		this.clearAllDatabasesBetweenTests = false;
		super.beforeEachTest();
	}

	@Test
	@Order(1)
	public void createGroup() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(defaultTestAdminUser);

		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser).addGroup(defaultTestAdminUser, TEST_GROUP,
					TEST_GROUP_TYPE, TEST_GROUP_DESCRIPTION, TEST_GROUP_IS_CUSTOM);
			assertTrue(true);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
		{
			List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroups(null, -1, -1);
			assertTrue(groupReturns.size() == 1);

			Map<String, Object> myGroup = groupReturns.get(0);
			assertTrue(myGroup.get("id").equals(TEST_GROUP));
			assertNull(myGroup.get("type"));
			assertTrue(myGroup.get("description").equals(TEST_GROUP_DESCRIPTION));
			assertTrue((boolean) myGroup.get("is_custom_group"));

			assertTrue(myGroup.get("userid").equals(userDetails.getValue0()));
			assertTrue(myGroup.get("useridtype").equals(userDetails.getValue1()));
			assertNotNull(myGroup.get("dateadded"));
		}
		// with a search term
		{
			List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroups("my", -1, -1);
			assertTrue(groupReturns.size() == 1);

			Map<String, Object> myGroup = groupReturns.get(0);
			assertTrue(myGroup.get("id").equals(TEST_GROUP));
			assertNull(myGroup.get("type"));
			assertTrue(myGroup.get("description").equals(TEST_GROUP_DESCRIPTION));
			assertTrue((boolean) myGroup.get("is_custom_group"));

			assertTrue(myGroup.get("userid").equals(userDetails.getValue0()));
			assertTrue(myGroup.get("useridtype").equals(userDetails.getValue1()));
			assertNotNull(myGroup.get("dateadded"));
		}
		// search not found
		{
			List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroups("z", -1, -1);
			assertTrue(groupReturns.isEmpty());
		}
		// offset is large
		{
			List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroups(null, -1, 10);
			assertTrue(groupReturns.isEmpty());
		}
	}

	@Test
	@Order(2)
	public void createGroupWithExistingName() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		assertThrows(IllegalArgumentException.class, 
				() -> {
					AdminSecurityGroupUtils.getInstance(defaultTestAdminUser).addGroup(defaultTestAdminUser, TEST_GROUP,
							TEST_GROUP_TYPE, TEST_GROUP_DESCRIPTION, TEST_GROUP_IS_CUSTOM);
					},
				"Group already exists"
				);
	}
	
	@Test
	@Order(3)
	public void editGroup() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(defaultTestAdminUser);

		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.editGroupAndPropagate(defaultTestAdminUser, TEST_GROUP, TEST_GROUP_TYPE, TEST_GROUP, TEST_GROUP_TYPE, "new description", true);
			
			{
				List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.getGroups(null, -1, -1);
				assertTrue(groupReturns.size() == 1);

				Map<String, Object> myGroup = groupReturns.get(0);
				assertTrue(myGroup.get("description").equals("new description"));
				
				// check the other stuff as well
				assertTrue(myGroup.get("id").equals(TEST_GROUP));
				assertNull(myGroup.get("type"));
				assertTrue((boolean) myGroup.get("is_custom_group"));
				assertTrue(myGroup.get("userid").equals(userDetails.getValue0()));
				assertTrue(myGroup.get("useridtype").equals(userDetails.getValue1()));
				assertNotNull(myGroup.get("dateadded"));
			}
			
			// reset it back 
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.editGroupAndPropagate(defaultTestAdminUser, TEST_GROUP, TEST_GROUP_TYPE, TEST_GROUP, TEST_GROUP_TYPE, TEST_GROUP_DESCRIPTION, true);
			{
				List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.getGroups(null, -1, -1);
				assertTrue(groupReturns.size() == 1);
				
				Map<String, Object> myGroup = groupReturns.get(0);
				assertTrue(myGroup.get("id").equals(TEST_GROUP));
				assertNull(myGroup.get("type"));
				assertTrue(myGroup.get("description").equals(TEST_GROUP_DESCRIPTION));
				assertTrue((boolean) myGroup.get("is_custom_group"));
				assertTrue(myGroup.get("userid").equals(userDetails.getValue0()));
				assertTrue(myGroup.get("useridtype").equals(userDetails.getValue1()));
				assertNotNull(myGroup.get("dateadded"));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	@Order(4)
	public void addGroupMember() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(defaultTestAdminUser);

		try {
			assertThrows(IllegalArgumentException.class, 
					() -> {
						AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
							.addUserToGroup(defaultTestAdminUser, TEST_GROUP, NATIVE_DUMMY_USER_ID, NATIVE_DUMMY_USER_PROVIDER.toString(), null);
						},
					"User " + NATIVE_DUMMY_USER_ID + " doesn't exist"
					);
			
			AccessToken newUser = new AccessToken();
			newUser.setId(NATIVE_DUMMY_USER_ID);
			newUser.setProvider(NATIVE_DUMMY_USER_PROVIDER);
			newUser.setName(NATIVE_DUMMY_NAME);
			newUser.setUsername(NATIVE_DUMMY_USERNAME);
			newUser.setEmail(NATIVE_DUMMY_EMAIL);
			
			assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, NATIVE_DUMMY_PASSWORD));

			// add user successfully
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.addUserToGroup(defaultTestAdminUser, TEST_GROUP, NATIVE_DUMMY_USER_ID, NATIVE_DUMMY_USER_PROVIDER.toString(), null);
			
			// add user again to get error
			assertThrows(IllegalArgumentException.class, 
					() -> {
						AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
							.addUserToGroup(defaultTestAdminUser, TEST_GROUP, NATIVE_DUMMY_USER_ID, NATIVE_DUMMY_USER_PROVIDER.toString(), null);
						},
					"User " + NATIVE_DUMMY_USER_ID + " already has access to group " + TEST_GROUP
					);
			
			// add user to group that doesn't exist to get error
			assertThrows(IllegalArgumentException.class, 
					() -> {
						AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
							.addUserToGroup(defaultTestAdminUser, "GROUP_DOESN'T_EXIST", NATIVE_DUMMY_USER_ID, NATIVE_DUMMY_USER_PROVIDER.toString(), null);
						},
					"Group GROUP_DOESN'T_EXIST does not exist"
					);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
		{
			List<Map<String, Object>> members = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroupMembers(TEST_GROUP, null, -1, -1);
			assertTrue(members.size() == 1);
			
			Map<String, Object> member = members.get(0);
			assertTrue(member.get("groupid").equals(TEST_GROUP));
			assertTrue(member.get("userid").equals(NATIVE_DUMMY_USER_ID));
			assertTrue(member.get("type").equals(NATIVE_DUMMY_USER_PROVIDER.toString()));
			assertNull(member.get("datadded"));
			assertNull(member.get("enddate"));
			assertTrue(member.get("permissiongrantedby").equals(userDetails.getValue0()));
			assertTrue(member.get("permissiongrantedbytype").equals(userDetails.getValue1()));
			
			assertTrue(member.get("name").equals(NATIVE_DUMMY_NAME));
			assertTrue(member.get("username").equals(NATIVE_DUMMY_USERNAME));
			assertTrue(member.get("email").equals(NATIVE_DUMMY_EMAIL));
		}
		// with a search term
		{
			List<Map<String, Object>> members = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroupMembers(TEST_GROUP, NATIVE_DUMMY_SEARCH, -1, -1);
			assertTrue(members.size() == 1);
			
			Map<String, Object> member = members.get(0);
			assertTrue(member.get("groupid").equals(TEST_GROUP));
			assertTrue(member.get("userid").equals(NATIVE_DUMMY_USER_ID));
			assertTrue(member.get("type").equals(NATIVE_DUMMY_USER_PROVIDER.toString()));
			assertNull(member.get("datadded"));
			assertNull(member.get("enddate"));
			assertTrue(member.get("permissiongrantedby").equals(userDetails.getValue0()));
			assertTrue(member.get("permissiongrantedbytype").equals(userDetails.getValue1()));
			
			assertTrue(member.get("name").equals(NATIVE_DUMMY_NAME));
			assertTrue(member.get("username").equals(NATIVE_DUMMY_USERNAME));
			assertTrue(member.get("email").equals(NATIVE_DUMMY_EMAIL));
		}
		// with bad search term
		{
			List<Map<String, Object>> members = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroupMembers(TEST_GROUP, "z", -1, -1);
			assertTrue(members.isEmpty());
		}
		// with large offset
		{
			List<Map<String, Object>> members = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroupMembers(TEST_GROUP, null, -1, 10);
			assertTrue(members.isEmpty());
		}
	}
	
	@Test
	@Order(5)
	public void deleteGroupMember() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.removeUserFromGroup(TEST_GROUP, NATIVE_DUMMY_USER_ID, NATIVE_DUMMY_USER_PROVIDER.toString());
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		// should have no users in group now
		List<Map<String, Object>> members = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.getGroupMembers(TEST_GROUP, null, -1, -1);
		assertTrue(members.isEmpty());
	}
	
	@Test
	@Order(6)
	public void searchForNonMembers() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		{
			List<Map<String, Object>> members = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getNonGroupMembers(TEST_GROUP, null, -1, -1);
			assertTrue(members.size() == 2);
		}
		// with a search term
		{
			List<Map<String, Object>> members = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getNonGroupMembers(TEST_GROUP, NATIVE_DUMMY_SEARCH, -1, -1);
			assertTrue(members.size() == 1);
			
			Map<String, Object> member = members.get(0);
			assertTrue(member.get("id").equals(NATIVE_DUMMY_USER_ID));
			assertTrue(member.get("type").equals(NATIVE_DUMMY_USER_PROVIDER.toString()));
			assertTrue(member.get("name").equals(NATIVE_DUMMY_NAME));
			assertTrue(member.get("username").equals(NATIVE_DUMMY_USERNAME));
			assertTrue(member.get("email").equals(NATIVE_DUMMY_EMAIL));
		}
		// with bad search term
		{
			List<Map<String, Object>> members = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getNonGroupMembers(TEST_GROUP, "z", -1, -1);
			assertTrue(members.isEmpty());
		}
		// with large offset
		{
			List<Map<String, Object>> members = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getNonGroupMembers(TEST_GROUP, null, -1, 10);
			assertTrue(members.isEmpty());
		}
	}
	
	@Test
	@Order(7)
	public void addGroupProjectPermission() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		try {
			SecurityProjectUtils.addProject(
					PERMISSION_TEST_PROJECTID, 
					PERMISSION_TEST_PROJECTID,
					IProject.PROJECT_TYPE.INSIGHTS.name(),
					"",
					false, 
					null,
					false,
					defaultTestAdminUser);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
		int testPermission = 1;
		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.addGroupProjectPermission(defaultTestAdminUser, TEST_GROUP, TEST_GROUP_TYPE, PERMISSION_TEST_PROJECTID, testPermission, null);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
		// try again
		assertThrows(IllegalArgumentException.class, 
				() -> {
					AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.addGroupProjectPermission(defaultTestAdminUser, TEST_GROUP, TEST_GROUP_TYPE, PERMISSION_TEST_PROJECTID, testPermission, null);
				},
				"Group " + TEST_GROUP + " already has access to project " + PERMISSION_TEST_PROJECTID + " with permission = " + AccessPermissionEnum.getPermissionValueById(testPermission)
				);
		
		{
			List<Map<String, Object>> projects = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getProjectsForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, -1, false);
			assertTrue(projects.size() == 1);
		}
		// with a search term
		{
			List<Map<String, Object>> projects = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getProjectsForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, -1, false);
			assertTrue(projects.size() == 1);
			
			Map<String, Object> thisProject = projects.get(0);
			assertTrue(thisProject.get("project_id").equals(PERMISSION_TEST_PROJECTID));
			assertTrue(thisProject.get("project_name").equals(PERMISSION_TEST_PROJECTID));
			assertTrue(thisProject.get("permission").equals(testPermission));
			//TODO: can add more keys
		}
		// with bad search term
		{
			List<Map<String, Object>> projects = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getProjectsForGroup(TEST_GROUP, TEST_GROUP_TYPE, "z", -1, -1, false);
			assertTrue(projects.isEmpty());
		}
		// with large offset
		{
			List<Map<String, Object>> projects = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getProjectsForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, 10, false);
			assertTrue(projects.isEmpty());
		}
	}
	
	@Test
	@Order(8)
	public void editGroupProjectPermission() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		try {
			final int testPermission = 2;

			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.editGroupProjectPermission(defaultTestAdminUser, 
						TEST_GROUP, TEST_GROUP_TYPE, 
						PERMISSION_TEST_PROJECTID, testPermission, null);
			
			{
				List<Map<String, Object>> projects = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.getProjectsForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, -1, false);
				assertTrue(projects.size() == 1);

				Map<String, Object> thisProject = projects.get(0);
				assertTrue(thisProject.get("project_id").equals(PERMISSION_TEST_PROJECTID));
				assertTrue(thisProject.get("project_name").equals(PERMISSION_TEST_PROJECTID));
				assertTrue(thisProject.get("permission").equals(testPermission));
			}
			
			// change it to existing value
			assertThrows(IllegalArgumentException.class, 
					() -> {
						AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
							.editGroupEnginePermission(defaultTestAdminUser, 
								TEST_GROUP, TEST_GROUP_TYPE, 
								PERMISSION_TEST_ENGINEID, testPermission, null);
					},
					"Group " + TEST_GROUP + " already has permission level " 
							+ AccessPermissionEnum.getPermissionValueById(testPermission) + " to project " + PERMISSION_TEST_ENGINEID
					);
			
			// try bad engine id
			assertThrows(IllegalArgumentException.class, 
					() -> {
						AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
							.editGroupEnginePermission(defaultTestAdminUser, 
								TEST_GROUP, TEST_GROUP_TYPE, 
								"projectIdNotExists", testPermission, null);
					},
					"Group " + TEST_GROUP + " does not currently have access to project projectIdNotExists to edit"
					);
			
			// reset it back 
			int origTestPermission = 1;

			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.editGroupProjectPermission(defaultTestAdminUser, 
						TEST_GROUP, TEST_GROUP_TYPE, 
						PERMISSION_TEST_PROJECTID, origTestPermission, null);
			{
				List<Map<String, Object>> projects = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.getProjectsForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, -1, false);
				assertTrue(projects.size() == 1);
				
				Map<String, Object> thisProject = projects.get(0);
				assertTrue(thisProject.get("project_id").equals(PERMISSION_TEST_PROJECTID));
				assertTrue(thisProject.get("project_name").equals(PERMISSION_TEST_PROJECTID));
				assertTrue(thisProject.get("permission").equals(origTestPermission));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	@Order(9)
	public void deleteGroupProjectPermission() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.removeGroupProjectPermission(defaultTestAdminUser, TEST_GROUP, TEST_GROUP_TYPE, PERMISSION_TEST_PROJECTID);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		// should have no users in group now
		List<Map<String, Object>> projects = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.getProjectsForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, -1, false);
		assertTrue(projects.isEmpty());
		
		// try bad engine id
		assertThrows(IllegalArgumentException.class, 
				() -> {
					AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.removeGroupEnginePermission(defaultTestAdminUser, 
							TEST_GROUP, TEST_GROUP_TYPE, "projectIdNotExists");
				},
				"Group " + TEST_GROUP + " does not currently have access to project projectIdNotExists to remove"
				);
	}
	
	@Test
	@Order(10)
	public void addGroupEnginePermission() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		try {
			SecurityEngineUtils.addEngine(
					PERMISSION_TEST_ENGINEID, 
					PERMISSION_TEST_ENGINEID, 
					IEngine.CATALOG_TYPE.DATABASE, 
					RdbmsTypeEnum.H2_DB.getLabel(), 
					"", 
					false, 
					defaultTestAdminUser);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
		int testPermission = 1;
		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.addGroupEnginePermission(defaultTestAdminUser, TEST_GROUP, TEST_GROUP_TYPE, PERMISSION_TEST_ENGINEID, testPermission, null);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
		// try again
		assertThrows(IllegalArgumentException.class, 
				() -> {
					AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.addGroupEnginePermission(defaultTestAdminUser, TEST_GROUP, TEST_GROUP_TYPE, PERMISSION_TEST_ENGINEID, testPermission, null);
				},
				"Group " + TEST_GROUP + " already has access to engine " + PERMISSION_TEST_ENGINEID + " with permission = " + AccessPermissionEnum.getPermissionValueById(testPermission)
				);
		
		{
			List<Map<String, Object>> engines = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getEnginesForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, -1);
			assertTrue(engines.size() == 1);
		}
		// with a search term
		{
			List<Map<String, Object>> engines = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getEnginesForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, -1);
			assertTrue(engines.size() == 1);
			
			Map<String, Object> thisEngine = engines.get(0);
			assertTrue(thisEngine.get("engine_id").equals(PERMISSION_TEST_ENGINEID));
			assertTrue(thisEngine.get("engine_name").equals(PERMISSION_TEST_ENGINEID));
			assertTrue(thisEngine.get("permission").equals(testPermission));
			//TODO: can add more keys
		}
		// with bad search term
		{
			List<Map<String, Object>> engines = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getEnginesForGroup(TEST_GROUP, TEST_GROUP_TYPE, "z", -1, -1);
			assertTrue(engines.isEmpty());
		}
		// with large offset
		{
			List<Map<String, Object>> engines = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getEnginesForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, 10);
			assertTrue(engines.isEmpty());
		}
	}
	
	@Test
	@Order(11)
	public void editGroupEnginePermission() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		try {
			final int testPermission = 2;

			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.editGroupEnginePermission(defaultTestAdminUser, 
						TEST_GROUP, TEST_GROUP_TYPE, 
						PERMISSION_TEST_ENGINEID, testPermission, null);
			
			{
				List<Map<String, Object>> engines = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.getEnginesForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, -1);
				assertTrue(engines.size() == 1);

				Map<String, Object> thisEngine = engines.get(0);
				assertTrue(thisEngine.get("engine_id").equals(PERMISSION_TEST_ENGINEID));
				assertTrue(thisEngine.get("engine_name").equals(PERMISSION_TEST_ENGINEID));
				assertTrue(thisEngine.get("permission").equals(testPermission));
			}
			
			// change it to existing value
			assertThrows(IllegalArgumentException.class, 
					() -> {
						AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
							.editGroupEnginePermission(defaultTestAdminUser, 
								TEST_GROUP, TEST_GROUP_TYPE, 
								PERMISSION_TEST_ENGINEID, testPermission, null);
					},
					"Group " + TEST_GROUP + " already has permission level " 
							+ AccessPermissionEnum.getPermissionValueById(testPermission) + " to engine " + PERMISSION_TEST_ENGINEID
					);
			
			// try bad engine id
			assertThrows(IllegalArgumentException.class, 
					() -> {
						AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
							.editGroupEnginePermission(defaultTestAdminUser, 
								TEST_GROUP, TEST_GROUP_TYPE, 
								"engineIdNotExists", testPermission, null);
					},
					"Group " + TEST_GROUP + " does not currently have access to engine engineIdNotExists to edit"
					);
			
			// reset it back 
			int origTestPermission = 1;

			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.editGroupEnginePermission(defaultTestAdminUser, 
						TEST_GROUP, TEST_GROUP_TYPE, 
						PERMISSION_TEST_ENGINEID, origTestPermission, null);
			{
				List<Map<String, Object>> engines = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.getEnginesForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, -1);
				assertTrue(engines.size() == 1);
				
				Map<String, Object> thisEngine = engines.get(0);
				assertTrue(thisEngine.get("engine_id").equals(PERMISSION_TEST_ENGINEID));
				assertTrue(thisEngine.get("engine_name").equals(PERMISSION_TEST_ENGINEID));
				assertTrue(thisEngine.get("permission").equals(origTestPermission));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	@Order(12)
	public void deleteGroupEnginePermission() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.removeGroupEnginePermission(defaultTestAdminUser, TEST_GROUP, TEST_GROUP_TYPE, PERMISSION_TEST_ENGINEID);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		// should have no users in group now
		List<Map<String, Object>> engines = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.getEnginesForGroup(TEST_GROUP, TEST_GROUP_TYPE, null, -1, -1);
		assertTrue(engines.isEmpty());
		
		// try bad engine id
		assertThrows(IllegalArgumentException.class, 
				() -> {
					AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.removeGroupEnginePermission(defaultTestAdminUser, 
							TEST_GROUP, TEST_GROUP_TYPE, "engineIdNotExists");
				},
				"Group " + TEST_GROUP + " does not currently have access to engine engineIdNotExists to remove"
				);
	}
	
	
	
	// perform at the end
	@Test
	@Order(1000)
	public void deleteGroup() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser).deleteGroupAndPropagate(TEST_GROUP, TEST_GROUP_TYPE);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
		// should have no groups left
		{
			List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroups(null, -1, -1);
			assertTrue(groupReturns.isEmpty());
		}
	}
}
