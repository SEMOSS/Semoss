package prerna.rdf.main;

import java.io.BufferedWriter;
import java.io.FileWriter;

import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;

class ModForms {

	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper();

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Forms_TAP_Core_Data.smss";
		BigDataEngine formsEngine = new BigDataEngine();
		formsEngine.openDB(engineProp);
		formsEngine.setEngineName("Forms_TAP_Core_Data");
		DIHelper.getInstance().setLocalProperty("Forms_TAP_Core_Data", formsEngine);

		String query = "SELECT DISTINCT ?system ?description WHERE {"
				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
				+ "{?system <http://semoss.org/ontologies/Relation/Contains/Description> ?description}"
				+ "}";

		final String LINE_SEPARATOR = "\n";
		String path = "C:\\workspace\\Semoss_Dev\\SystemDescriptions.tsv";
		// set up csv path

		FileWriter writer = new FileWriter(path);
		BufferedWriter bufferedWriter = new BufferedWriter(writer);

		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(formsEngine, query);
		int count = 0;
		while(manager.hasNext()) {
			Object[] row = manager.next().getValues();
			StringBuilder sb = new StringBuilder();
			sb.append(row[0]).append("\t").append(row[1].toString().replaceAll("\t", "_").replaceAll("\n", "_").replaceAll("\r", "_")).append(LINE_SEPARATOR);
			bufferedWriter.write(sb.toString());
			count++;
		}
		bufferedWriter.close();
		writer.close();
		
		System.out.println("COUNT ::: " + count);
		System.out.println("DONE");
	}

}
