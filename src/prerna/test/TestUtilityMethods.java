package prerna.test;

import java.sql.SQLException;

import org.apache.log4j.PropertyConfigurator;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.DIHelper;

public final class TestUtilityMethods {

	private TestUtilityMethods() {

	}

	public static void loadDIHelper() {
		String workingDir = System.getProperty("user.dir");
		String propFile = workingDir + "/RDF_Map.prop";
		loadDIHelper(propFile);
	}

	public static void loadDIHelper(String propFile) {
		DIHelper.getInstance().loadCoreProp(propFile);
		//Set log4j prop
		String log4jConfig = DIHelper.getInstance().getProperty("LOG4J");
		PropertyConfigurator.configure(log4jConfig);
	}

	public static void loadLocalMasterAndSecruity() throws Exception {
		String engineProp = "C:\\workspace2\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("LocalMasterDatabase");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);

		engineProp = "C:\\workspace2\\Semoss_Dev\\db\\security.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("security");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("security", coreEngine);
		try {
			AbstractSecurityUtils.loadSecurityDatabase();
		} catch (SQLException e) {
			e.printStackTrace();
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
