package prerna.test;

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
		DIHelper.getInstance().loadCoreProp(propFile);
	}
	
	public static void loadDIHelper(String propFile) {
		DIHelper.getInstance().loadCoreProp(propFile);
	}
	
	public static void loadLocalMasterAndSecruity() {
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("LocalMasterDatabase");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);
		
		engineProp = "C:\\workspace\\Semoss_Dev\\db\\security.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("security");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("security", coreEngine);
		AbstractSecurityUtils.loadSecurityDatabase();
	}
	
	public static void loadAll(String propFile) {
		loadDIHelper(propFile);	
		loadLocalMasterAndSecruity();
	}
	
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();
		System.out.println(DIHelper.getInstance().getProperty("BaseFolder"));
	}
}
