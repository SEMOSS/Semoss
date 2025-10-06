package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import org.eclipse.egit.github.core.client.GsonUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.testing.ApiTestsSemossConstants;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SecurityPasswordResetUtilsUnitTests {
	@TempDir
	static File tempDir;
	
	private static String id = "test123";
	private static String email = "test123@test.com";
	private static String type = "NATIVE";

	@BeforeAll
	static void createTempDbFolder() throws Exception {
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();

		// set up base folders
		File baseFolder = new File(tempDir, "baseFolder");
		baseFolder.mkdir();
		File dbFolder = new File(baseFolder, "db");
		dbFolder.mkdir();

		// creating temp rdf file for DI Helper
		String rdfMap = tempDir + fileSeparator + "rdfMap.prop";
		File rdfMapFile = new File(tempDir + fileSeparator + "rdfMap.prop");
		Properties rdfMapProps = new Properties();
		rdfMapProps.setProperty(Constants.BASE_FOLDER, baseFolder.getAbsolutePath().toString());

		// save rdf map file
		try (FileOutputStream out = new FileOutputStream(rdfMapFile)) {
			rdfMapProps.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}
		DIHelper.getInstance().loadCoreProp(rdfMap);

		// adding security db to temp DI Helper
		File securityFolder = new File(dbFolder, "security");
		securityFolder.mkdir();

		// copy smss file to temp db folder
		String smssPath = ApiTestsSemossConstants.TEST_DB_DIRECTORY + fileSeparator + "security.smss";
		Path sourcePath = Paths.get(smssPath);
		Path secSmss = Paths.get(dbFolder.getAbsolutePath() + fileSeparator + "security.smss");
		Files.copy(sourcePath, secSmss, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
		DIHelper instance = DIHelper.getInstance();
		RDBMSNativeEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("security");
		coreEngine.open(secSmss.toString());
		instance.setEngineProperty("security_" + Constants.STORE, secSmss.toAbsolutePath().toString());

		AbstractSecurityUtils.loadSecurityDatabase();

		// add user to security DB
		String name = "Test User";
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

		boolean success = SecurityUpdateUtils.registerUser(id, name, email, password, type, phone, phoneextension,
				countrycode, admin, publisher, exporter, modelUsageRestriction, modelUsageFrequency, modelMaxTokens,
				modelMaxResponseTime);
		
		assertTrue(success, "Insertion of new user should be successful");
	}

	@Test
	void testUserEmailExists() throws Exception {
		boolean exists = SecurityPasswordResetUtils.userEmailExists(email, type);
		assertTrue(exists);
	}

	@Test
	void testGetUserIdFromEmail() throws Exception {
		String userId = SecurityPasswordResetUtils.getUserIdFromEmail(email, type);
		assertEquals(id, userId);
	}
	
	@Test
	void testAllowUserResetPassword() throws Exception {
		String token = SecurityPasswordResetUtils.allowUserResetPassword(email, type);
		assertTrue(token != null && !token.isEmpty());
	}
	
	@Test
	void testUserResetPassword() throws Exception {
		String token = SecurityPasswordResetUtils.allowUserResetPassword(email, type);
		String newPassword = "newPass123!";
		Map<String, Object> retMap = SecurityPasswordResetUtils.userResetPassword(token, newPassword);
		System.out.println(GsonUtils.getGson().toJson(retMap));
		assertFalse(retMap.isEmpty());
		assertEquals(id, retMap.get("userId"));
		assertEquals(email, retMap.get("email"));

	}

	@AfterAll
	static void tearDown() throws IOException {
		IEngine secDb = Utility.getEngine("security");
		secDb.close();
	}
}
