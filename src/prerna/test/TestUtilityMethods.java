package prerna.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public final class TestUtilityMethods {
	
	private static final Logger classLogger = LogManager.getLogger(TestUtilityMethods.class);

	private TestUtilityMethods() {

	}

	
	protected static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public static void loadDIHelper() {
		String workingDir = System.getProperty("user.dir");
		String propFile = workingDir + "/RDF_Map.prop";
		loadDIHelper(propFile);
	}

	public static void loadDIHelper(String propFile) {
		DIHelper.getInstance().loadCoreProp(propFile);
		//Set log4j prop
		String log4JPropFile = new File(Utility.normalizePath(propFile)).getParent() + FILE_SEPARATOR +"log4j2.properties";
		FileInputStream fis = null;
		ConfigurationSource source = null;
		try {
			fis = new FileInputStream(log4JPropFile);
			source = new ConfigurationSource(fis);
			Configurator.initialize(null, source);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	public static void loadLocalMasterAndSecruity() throws Exception {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		String engineProp = baseFolder + "\\db\\LocalMasterDatabase.smss";
		IDatabaseEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("LocalMasterDatabase");
		coreEngine.open(engineProp);
		DIHelper.getInstance().setEngineProperty("LocalMasterDatabase", coreEngine);

		engineProp = baseFolder + "\\db\\security.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("security");
		coreEngine.open(engineProp);
		DIHelper.getInstance().setEngineProperty("security", coreEngine);
		try {
			AbstractSecurityUtils.loadSecurityDatabase();
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	public static void loadAll(String propFile) throws Exception {
		loadDIHelper(propFile);	
		loadLocalMasterAndSecruity();
	}

	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();
		System.out.println(DIHelper.getInstance().getProperty("BaseFolder"));
	}
}
