package prerna.rdf.main;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;

class TestGson {
	
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\test5.smss";
		BigDataEngine test5 = new BigDataEngine();
		test5.openDB(engineProp);
		test5.setEngineName("test5");
		DIHelper.getInstance().setLocalProperty("test5", test5);

		String query = "select distinct ?s ?p ?o where {?s ?p ?o}";
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(test5, query);
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String sub = ss.getRawVar("s") + "";
			String pred = ss.getRawVar("p") + "";
			String obj = ss.getRawVar("o") + "";

			System.out.println(sub + " >>> " + pred + " >>> " + obj);
		}
		
	}
	
}
