package prerna.engine.impl.r;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.RInterpreter;
import prerna.testing.ApiTestsSemossConstants;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RNativeEngineUnitTests {
		
//	@Test
//	public void testOpen(@TempDir File tempDir) throws Exception {
//		// make base folders for the db
//		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
//		String baseFolderPath = "baseFolder";
//		File baseFolder = new File(tempDir, baseFolderPath);
//		baseFolder.mkdir();
//		File baseDBFolder = new File(tempDir, baseFolderPath + fileSeparator + "db");
//		baseDBFolder.mkdir();
//		
//		// testing setup
//		String engineId = "engineId";
//		String engineName = "RNativeTest";
//		File rDBFolder = new File(baseDBFolder, engineName+"__"+engineId);
//		rDBFolder.mkdir();
//		
//		File rDBFolderAppRoot = new File(rDBFolder, "app_root/version/assets");
//		rDBFolderAppRoot.mkdirs();
//
//		// copy movies csv to temp folder
//		Path movieCsv = ApiTestsSemossConstants.TEST_MOVIE_CSV_PATH;
//		File newCSVFile = new File(rDBFolderAppRoot + fileSeparator + ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME);
//		Files.copy(movieCsv, newCSVFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
//
//		String smssFilePath = engineName + "__" + engineId + ".smss";
//		File dbSMSS = new File(baseDBFolder, smssFilePath);
//
//		// creating RNative smss prop file
//		Properties smssProp = new Properties();
//		smssProp.setProperty(Constants.ENGINE, engineId);
//		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
//		smssProp.setProperty(AbstractDatabaseEngine.DATA_FILE,
//				"@BaseFolder@/db/@ENGINE@/app_root/version/assets/" + ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME);
//		smssProp.setProperty(Constants.OWL, "REMAKE");
//
//		// save prop file
//		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
//			smssProp.store(out, "Properties");
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
//			DIHelper instance = Mockito.mock(DIHelper.class);
//			when(DIHelper.getInstance()).thenReturn(instance);
//			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());
//
//			// testing open
//			RNativeEngine re = new RNativeEngine();
//			re.open(smssProp);
//
//			// validations
//			assertEquals(engineId, re.getEngineId());
//			
//			re.close();
//		}
//	}
	
	@Test
	void testGetDatabaseType() {
		RNativeEngine re = new RNativeEngine();
		assertEquals(IDatabaseEngine.DATABASE_TYPE.R, re.getDatabaseType());
	}
	
	@Test
	void testGetQueryInterpreter() {
		RNativeEngine re = new RNativeEngine();
		IQueryInterpreter interpreter = re.getQueryInterpreter();
		assertTrue( interpreter instanceof RInterpreter);
	}
}
