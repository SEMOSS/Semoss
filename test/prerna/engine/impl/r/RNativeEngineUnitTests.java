package prerna.engine.impl.r;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.engine.impl.tinker.TinkerEngine.TINKER_DRIVER;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RNativeEngineUnitTests {

	@Test
	public void testOpen(@TempDir File tempDir) throws Exception {
		// make base folders for the db
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "baseFolder";
		File baseFolder = new File(tempDir, baseFolderPath);
		baseFolder.mkdir();
		File baseDBFolder = new File(tempDir, baseFolderPath + fileSeparator + "db");
		baseDBFolder.mkdir();

		// testing setup
		String engineId = "engineId";
		String engineName = "RNativeTest";
		String smssFilePath = engineName + "__" + engineId + ".smss";
		File dbSMSS = new File(baseDBFolder, smssFilePath);

		// creating RNative smss prop file
		Properties smssProp = new Properties();
		smssProp.setProperty(Constants.ENGINE, engineId);
		smssProp.setProperty(Constants.ENGINE_ALIAS, engineName);
		smssProp.setProperty(AbstractDatabaseEngine.DATA_FILE, "RFILE");

		// save prop file
		try (FileOutputStream out = new FileOutputStream(dbSMSS)) {
			smssProp.store(out, "Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

			// testing open
			RNativeEngine re = new RNativeEngine();
			re.open(smssProp);

			// validations
			assertEquals(engineId, re.getEngineId());
			
			re.close();
		}
	}
}
