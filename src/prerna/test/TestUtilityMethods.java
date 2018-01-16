package prerna.test;

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
	
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();
		System.out.println(DIHelper.getInstance().getProperty("BaseFolder"));
	}
}
