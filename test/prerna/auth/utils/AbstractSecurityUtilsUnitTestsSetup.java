package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import prerna.SemossUnitTest;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AbstractSecurityUtilsUnitTestsSetup extends SemossUnitTest {
    
    static Path securityOwlFile = null;
    static String databaseFolder = null;
    static Path dbDir = null;

    private static final String fileSeparator = FileSystems.getDefault().getSeparator();

	@BeforeAll
	public static void createTempDbFolder() throws Exception {
		// set up base folders
		File baseFolder = new File(tempDir.toFile(), "semoss");
		baseFolder.mkdir();
		File dbFolder = new File(baseFolder, "db");
		dbFolder.mkdir();
        databaseFolder = dbFolder.getAbsolutePath();
        dbDir = dbFolder.toPath();

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
        Properties securityProps = UnitTestSecurityAuthUtils.getDefaultDBProperties("security");

        Path secSmss = UnitTestSecurityAuthUtils.createSmssFileFromProps(securityProps, dbFolder.getAbsolutePath(),
                "security.smss");

		DIHelper instance = DIHelper.getInstance();
		RDBMSNativeEngine securityDB = new RDBMSNativeEngine();
		securityDB.setEngineId("security");
		securityDB.open(secSmss.toString());
		instance.setEngineProperty("security_" + Constants.STORE, secSmss.toAbsolutePath().toString());

        securityOwlFile = securityFolder.toPath()
                .resolve("app_root")
                .resolve("version")
                .resolve("assets")
                .resolve("security_OWL.OWL");

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
