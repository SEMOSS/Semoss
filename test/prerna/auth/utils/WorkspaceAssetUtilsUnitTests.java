package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

//	@Test
//	void testCreateUserAssetProject() throws Exception {
//		String projectId = WorkspaceAssetUtils.createUserAssetProject(USER, ap);
//		assertNotNull(projectId);
//		assertTrue(!projectId.isEmpty());
//		IProject project = Utility.getProject(projectId);
//		assertEquals(WorkspaceAssetUtils.ASSET_APP_NAME, project.getEngineName());
//		project.close();
//	}

//	@Test
//	void testCreateEmptyProject() throws Exception {
//
//	}
//
	@Test
	void testRegisterUserWorkspaceProjectToken() throws Exception {
		String projectId = "projectID123";
		WorkspaceAssetUtils.registerUserWorkspaceProject(USER.getAccessToken(ap), projectId);
		
		// validate 
		assertEquals(projectId, WorkspaceAssetUtils.getUserWorkspaceProject(USER.getAccessToken(ap)));
	}

	@Test
	void testRegisterUserWorkspaceProject() throws Exception {
		String projectId = "projectID123";
		WorkspaceAssetUtils.registerUserWorkspaceProject(USER, ap, projectId);
		
		// validate
		assertEquals(projectId, WorkspaceAssetUtils.getUserWorkspaceProject(USER.getAccessToken(ap)));
	}

	@Test
	void testRegisterUserAssetProjectToken() throws Exception {
		String projectId = "projectID123";
		WorkspaceAssetUtils.registerUserAssetProject(USER.getAccessToken(ap), projectId);
		
		// validate 
		assertEquals(projectId, WorkspaceAssetUtils.getUserAssetProject(USER.getAccessToken(ap)));
	}
//
	@Test
	void testRegisterUserAssetProject() throws Exception {
		String projectId = "projectID123";
		WorkspaceAssetUtils.registerUserAssetProject(USER, ap, projectId);
		
		// validate 
		assertEquals(projectId, WorkspaceAssetUtils.getUserAssetProject(USER.getAccessToken(ap)));
	}

	@Test
	void testGetUserAssetProjectToken() throws Exception {
		String projectId = "projectID123";
		
		User user2 = new User();
		AccessToken at = new AccessToken();
		at.setProvider(ap);
		at.setId("userName2");
		at.setEmail("email2@test.com");
		user2.setAccessToken(at);
		user2.setPrimaryLogin(ap);
		
		assertEquals(null, WorkspaceAssetUtils.getUserAssetProject(at));
	}

	@Test
	void testGetUserAssetProject() throws Exception {
		User user2 = new User();
		AccessToken at = new AccessToken();
		at.setProvider(ap);
		at.setId("userName2");
		at.setEmail("email2@test.com");
		user2.setAccessToken(at);
		user2.setPrimaryLogin(ap);
		
		assertEquals(null, WorkspaceAssetUtils.getUserAssetProject(user2, ap));
	}

	@Test
	void testIsAssetOrWorkspaceProject() throws Exception {
		String projectId = "projectID44";
		WorkspaceAssetUtils.registerUserWorkspaceProject(USER, ap, projectId);
		boolean isWorkspaceEngine = WorkspaceAssetUtils.isAssetOrWorkspaceProject(projectId);
		assertTrue(isWorkspaceEngine);
		
		isWorkspaceEngine = WorkspaceAssetUtils.isAssetOrWorkspaceProject("bad");
		assertFalse(isWorkspaceEngine);
	}
	
	@Test
	void testIsAssetProject() throws Exception {
		String projectId = "projectID33322";
		WorkspaceAssetUtils.registerUserAssetProject(USER, ap, projectId);
		
		boolean isWorkspaceEngine = WorkspaceAssetUtils.isAssetProject(projectId);
		assertTrue(isWorkspaceEngine);
		
		isWorkspaceEngine = WorkspaceAssetUtils.isAssetProject("bad");
		assertFalse(isWorkspaceEngine);
	}
	
//	@Test
	void testGetUserAssetRootDirectory() throws Exception {
		String projectId = WorkspaceAssetUtils.createUserWorkspaceProject(USER, ap);
		String folder = WorkspaceAssetUtils.getUserAssetRootDirectory(USER, ap);
		assertTrue(folder.startsWith(tempDir+"/baseFolder/project/Asset__"+projectId));
	}

}
