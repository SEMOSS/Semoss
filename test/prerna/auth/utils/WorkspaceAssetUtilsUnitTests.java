package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.project.api.IProject;
import prerna.util.Utility;

public class WorkspaceAssetUtilsUnitTests extends AbstractSecurityUtilsUnitTests {
	private static User USER = null;
	private static String userName = "userName";
	private static String email = "email";
	private static AuthProvider ap = AuthProvider.NATIVE;

	@BeforeAll
	static void setUp() throws Exception {
		USER = new User();
		AccessToken at = new AccessToken();
		at.setProvider(ap);
		at.setId(userName);
		at.setEmail(email);
		USER.setAccessToken(at);
		USER.setPrimaryLogin(ap);
	}

	@Test
	void testCreateUserWorkspaceProject() throws Exception {
		String projectId = WorkspaceAssetUtils.createUserWorkspaceProject(USER, ap);

		// assert workspace project in user path
		String workspaceUserProjectPath = WorkspaceAssetUtilsUnitTests.tempDir + "\\baseFolder\\user\\Workspace__"
				+ projectId;
		File testFile = new File(workspaceUserProjectPath);
		assertTrue(testFile.exists());
		assertTrue(testFile.isDirectory());

		// assert workspace smss in user path
		String workspaceUserProjectSMSSPath = WorkspaceAssetUtilsUnitTests.tempDir + "\\baseFolder\\user\\Workspace__"
				+ projectId + ".smss";
		testFile = new File(workspaceUserProjectSMSSPath);
		assertTrue(testFile.exists());

		// assert workspace project folder
		String workspaceProjectPath = WorkspaceAssetUtilsUnitTests.tempDir + "\\baseFolder\\project\\Workspace__"
				+ projectId;
		testFile = new File(workspaceProjectPath);
		assertTrue(testFile.exists());
		assertTrue(testFile.isDirectory());

		assertNotNull(projectId);
		assertTrue(!projectId.isEmpty());
		IProject project = Utility.getProject(projectId);
		assertEquals(WorkspaceAssetUtils.WORKSPACE_APP_NAME, project.getEngineName());
		project.close();
	}

	@Test
	void testCreateUserAssetProject() throws Exception {
		String projectId = WorkspaceAssetUtils.createUserAssetProject(USER, ap);
		assertNotNull(projectId);
		assertTrue(!projectId.isEmpty());
		IProject project = Utility.getProject(projectId);
		assertEquals(WorkspaceAssetUtils.ASSET_APP_NAME, project.getEngineName());
		project.close();
	}

	@Test
	void testCreateEmptyProject() throws Exception {

	}

	@Test
	void testRegisterUserWorkspaceProjectToken() throws Exception {

	}

	@Test
	void testRegisterUserWorkspaceProject() throws Exception {

	}

	@Test
	void testRegisterUserAssetProjectToken() throws Exception {

	}

	@Test
	void testRegisterUserAssetProject() throws Exception {

	}

	@Test
	void testGetUserWorkspaceProjectToken() throws Exception {

	}

	@Test
	void testGetUserWorkspaceProject() throws Exception {

	}

	@Test
	void testGetUserAssetProjectToken() throws Exception {

	}

	@Test
	void testGetUserAssetProject() throws Exception {

	}

	@Test
	void testIsAssetOrWorkspaceProject() throws Exception {

	}

	@Test
	void testIsAssetProject() throws Exception {

	}

	@Test
	void testGetUserAssetRootDirectory() throws Exception {

	}

}
