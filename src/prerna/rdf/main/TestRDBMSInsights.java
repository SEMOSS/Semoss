package prerna.rdf.main;

import java.util.Properties;
import java.util.Vector;

import prerna.om.SEMOSSParam;
import prerna.util.DIHelper;

public class TestRDBMSInsights {

	public static void main(String[] args) {
		
		Properties diProp = new Properties();
		diProp.put("BaseFolder", "C:\\workspace\\Semoss");
		DIHelper.getInstance().setCoreProp(diProp);

		prerna.engine.impl.rdf.BigDataEngine e = new prerna.engine.impl.rdf.BigDataEngine();
		String propFile = "C:\\workspace\\Semoss\\db\\Movie_DB.smss";
		e.openDB(propFile);
		
		Vector<SEMOSSParam> results2 = e.getParams("Movie_DB_18");
		for(SEMOSSParam r : results2) {
			System.out.println(r);
		}
		
	}
	
}
