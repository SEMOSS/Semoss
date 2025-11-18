package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import prerna.SemossUnitTest;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.testing.ApiTestsSemossConstants;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AbstractSecurityUtilsUnitTests extends SemossUnitTest {

	@BeforeAll
	public static void createTempDbFolder() throws Exception {
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();

		// set up base folders
		File baseFolder = new File(tempDir.toFile(), "semoss");
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
		RDBMSNativeEngine securityDB = new RDBMSNativeEngine();
		securityDB.setEngineId("security");
		securityDB.open(secSmss.toString());
		instance.setEngineProperty("security_" + Constants.STORE, secSmss.toAbsolutePath().toString());

		AbstractSecurityUtils.loadSecurityDatabase();

	}

	@AfterAll
	public static void tearDown() throws IOException, SQLException {
		RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		try (Connection c = securityDb.getConnection(); Statement s = c.createStatement()) {
			assertTrue(c.getMetaData().getURL().contains("junit"));
			s.execute("SHUTDOWN");
		}
		securityDb.closeDataSource();
		securityDb.close();
		securityDb.delete();

        // clear properties
        DIHelper helper = DIHelper.getInstance();
        helper = null;
        assertNull(DIHelper.getInstance().getEngineProperty("security_STORE"));
	}
}
