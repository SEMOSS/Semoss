package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.threeten.bp.ZonedDateTime;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.project.api.IProject;
import prerna.project.api.IProject.PROJECT_TYPE;
import prerna.project.impl.ProjectHelper;
import prerna.testing.ApiSemossTestUtils;

public class SecurityUserProjectUtilsUnitTests extends AbstractSecurityUtilsUnitTests {
	private static User user = new User();
	private static User user2 = new User();

	private static final Logger classLogger = LogManager.getLogger(SecurityUserProjectUtilsUnitTests.class);

	private static String projectId = null;

	@BeforeAll
	static void setup() throws Exception {
		String user1Id = "user1";
		String user2Id = "user2";

		// add user 1
		{
			String email = "user1@test.com";
			String type = "NATIVE";
			String name = "Test1 User1";
			String password = "password123";
			String phone = "5551234567";
			String phoneextension = "001";
			String countrycode = "US";
			boolean admin = true;
			boolean publisher = false;
			boolean exporter = false;
			String modelUsageRestriction = null;
			String modelUsageFrequency = null;
			Integer modelMaxTokens = null;
			Double modelMaxResponseTime = null;

			boolean success = SecurityUpdateUtils.registerUser(user1Id, name, email, password, type, phone,
					phoneextension, countrycode, admin, publisher, exporter, modelUsageRestriction, modelUsageFrequency,
					modelMaxTokens, modelMaxResponseTime);
			assertTrue(success, "Insertion of new user should be successful");

			AccessToken at = new AccessToken();
			AuthProvider ap = AuthProvider.NATIVE;
			at.setProvider(ap);
			at.setId(user1Id);
			at.setEmail(email);
			user.setAccessToken(at);
			user.setPrimaryLogin(ap);
		}

		// add user2
		{
			String email = "user12@test.com";
			String type = "NATIVE";
			String name = "Test2 User2";
			String password = "password123456";
			String phone = "2222222222";
			String phoneextension = "001";
			String countrycode = "US";
			boolean admin = false;
			boolean publisher = false;
			boolean exporter = false;
			String modelUsageRestriction = null;
			String modelUsageFrequency = null;
			Integer modelMaxTokens = null;
			Double modelMaxResponseTime = null;

			boolean success = SecurityUpdateUtils.registerUser(user2Id, name, email, password, type, phone,
					phoneextension, countrycode, admin, publisher, exporter, modelUsageRestriction, modelUsageFrequency,
					modelMaxTokens, modelMaxResponseTime);
			assertTrue(success, "Insertion of new user should be successful");

			AccessToken at = new AccessToken();
			AuthProvider ap = AuthProvider.NATIVE;
			at.setProvider(ap);
			at.setId(user2Id);
			at.setEmail(email);
			user2.setAccessToken(at);
			user2.setPrimaryLogin(ap);
		}

		// add project
		String projectName = "testProject";
		PROJECT_TYPE projectType = PROJECT_TYPE.INSIGHTS;
		boolean global = false;
		boolean hasPortal = false;
		String portalName = "testPortal";
		String gitProvider = null;
		String gitCloneUrl = null;
		IProject project = ProjectHelper.generateNewProject(projectName, projectType, global, hasPortal, portalName,
				gitProvider, gitCloneUrl, user, classLogger);
		projectId = project.getProjectId();

		// add group
		String groupId = "group123";
		String groupType = "CUSTOM";
		AdminSecurityGroupUtils.getInstance(user).addGroup(user, groupId, groupType, "group desc");

		// add project to group
		SecurityGroupProjectUtils.addProjectGroupPermission(user, groupId, groupType, projectId,
				AccessPermissionEnum.EDIT.getPermission(), null);

		// add user 2 to group
		String endDate = ZonedDateTime.now().minusDays(4).toString();
		AdminSecurityGroupUtils.getInstance(user).addUserToGroup(user, groupId, user2Id, "NATIVE", endDate);

	}

	@Test
	void testGetFullUserProjectIds() {
		List<String> list = SecurityUserProjectUtils.getFullUserProjectIds(user);
		assertEquals(1, list.size());
		assertTrue(list.contains(projectId));
	}

	@Test
	void testGetActualUserProjectPermission() {
		String permission = SecurityUserProjectUtils.getActualUserProjectPermission(user, projectId);
		assertEquals(AccessPermissionEnum.OWNER.getPermission(), permission);

		permission = SecurityUserProjectUtils.getActualUserProjectPermission(user2, projectId);
		assertEquals(null, permission);
	}

	@Test
	void testGetActualGroupUserProjectPermission() {
		List<String> groupPermissions = SecurityUserProjectUtils.getActualGroupUserProjectPermission(user2, projectId);
		assertEquals(1, groupPermissions.size());
		assertTrue(groupPermissions.contains(AccessPermissionEnum.EDIT.getPermission()));

		groupPermissions = SecurityUserProjectUtils.getActualGroupUserProjectPermission(user, projectId);
		assertEquals(0, groupPermissions.size());
//		assertTrue(groupPermissions.contains(AccessPermissionEnum.OWNER.getPermission()));
	}

	@Test
	void testGetHighestProjectPermission() {
		String userPermission = AccessPermissionEnum.EDIT.getPermission();
		List<String> groupPermissions = SecurityUserProjectUtils.getActualGroupUserProjectPermission(user2, projectId);

		String highestPermission = SecurityUserProjectUtils.getHighestProjectPermission(userPermission,
				groupPermissions);
		assertEquals(AccessPermissionEnum.EDIT.getPermission(), highestPermission);

	}

	@Test
	void testGetUserProjectPermission() {
		Integer permissionInt = SecurityUserProjectUtils.getUserProjectPermission(user.getPrimaryLoginToken().getId(),
				projectId);
		assertEquals(1, permissionInt);

		permissionInt = SecurityUserProjectUtils.getUserProjectPermission(user, projectId);
		assertEquals(1, permissionInt);

		permissionInt = SecurityUserProjectUtils.getUserProjectPermission(user2, projectId);
		assertEquals(null, permissionInt);
	}

	@Test
	void testUserIsOwner() {
		boolean isOwner = SecurityUserProjectUtils.userIsOwner(user, projectId);
		assertTrue(isOwner);

		Vector<String> userIds = new Vector<>();
		userIds.add(user.getPrimaryLoginToken().getId());
		isOwner = SecurityUserProjectUtils.userIsOwner(userIds, projectId);
		assertTrue(isOwner);

		isOwner = SecurityUserProjectUtils.userIsOwner(user2, projectId);
		assertFalse(isOwner);
	}

	@Test
	void testUserCanViewProject() {
		boolean canView = SecurityUserProjectUtils.userCanViewProject(user, projectId);
		assertTrue(canView);

		canView = SecurityUserProjectUtils.userCanViewProject(user2, projectId);
		assertFalse(canView);
	}

	@Test
	void testUserCanEditProject() {
		boolean canEdit = SecurityUserProjectUtils.userCanEditProject(user, projectId);
		assertTrue(canEdit);

		canEdit = SecurityUserProjectUtils.userCanViewProject(user2, projectId);
		assertFalse(canEdit);
		
		canEdit = SecurityUserProjectUtils.userCanViewProject(null, null);
		assertFalse(canEdit);
	}

	@Test
	void testGetMaxUserProjectPermission() {
		Integer maxPermission = SecurityUserProjectUtils.getMaxUserProjectPermission(user, projectId);
		assertEquals(1, maxPermission);

		maxPermission = SecurityUserProjectUtils.getMaxUserProjectPermission(user2, projectId);
		assertEquals(3, maxPermission);
		
		maxPermission = SecurityUserProjectUtils.getMaxUserProjectPermission(null, null);
		assertEquals(3, maxPermission);

	}

	@Test
	void testCheckUserHasAccessToProject() throws Exception {
		boolean hasAccess = SecurityUserProjectUtils.checkUserHasAccessToProject(projectId,
				user.getPrimaryLoginToken().getId());
		assertTrue(hasAccess);

		hasAccess = SecurityUserProjectUtils.checkUserHasAccessToProject(projectId,
				user2.getPrimaryLoginToken().getId());
		assertFalse(hasAccess);
		hasAccess = SecurityUserProjectUtils.checkUserHasAccessToProject(null,
				null);
		assertFalse(hasAccess);
	}

	@Test
	void testGetProjectUsers() throws Exception {
		String searchParam = null;
		String permission = null;
		long limit = 10;
		long offset = 0;
		List<Map<String, Object>> users = SecurityUserProjectUtils.getProjectUsers(projectId, searchParam, permission,
				limit, offset);
		assertTrue(!users.isEmpty());
		ApiSemossTestUtils.print(users);
	}

	@Test
	void testProjectPermissionIsExpired() throws Exception {
		boolean hasExpired = SecurityUserProjectUtils.projectPermissionIsExpired(user.getPrimaryLoginToken().getId(),
				projectId);
		assertFalse(hasExpired);

		hasExpired = SecurityUserProjectUtils.projectPermissionIsExpired(user2.getPrimaryLoginToken().getId(),
				projectId);
		assertFalse(hasExpired);
	}
}
