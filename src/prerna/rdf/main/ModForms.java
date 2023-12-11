package prerna.rdf.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.ACTION_TYPE;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.Utility;

class ModForms {

//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadDIHelper();
//
//		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Forms_TAP_Core_Data.smss";
//		BigDataEngine formsEngine = new BigDataEngine();
//		formsEngine.open(engineProp);
//		formsEngine.setEngineId("Forms_TAP_Core_Data");
//		DIHelper.getInstance().setLocalProperty("Forms_TAP_Core_Data", formsEngine);
//
//		String testQ = "SELECT DISTINCT ?system (COUNT(?description) as ?count) WHERE {"
//				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
//				+ "{?system <http://semoss.org/ontologies/Relation/Contains/Description> ?description}"
//				+ "} GROUP BY ?system";
//		
//		System.out.println("TEST DUPLICATE DESCRIPTIONS !!! ");
//		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(formsEngine, testQ);
//		while(manager.hasNext()) {
//			Object[] values = manager.next().getValues();
//			if( ((Number) values[1]).intValue() > 1 ) {
//				System.out.println("SYSTEM ::: " + values[0]);
//			}
//		}
//		System.out.println("DONE TEST DUPLICATE DESCRIPTIONS !!! ");
//
////		String query = "SELECT DISTINCT ?system ?description WHERE {"
////				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
////				+ "{?system <http://semoss.org/ontologies/Relation/Contains/Description> ?description}"
////				+ "}";
////
////		final String LINE_SEPARATOR = "\n";
////		String path = "C:\\workspace\\Semoss_Dev\\SystemDescriptions.tsv";
////		// set up csv path
////
////		FileWriter writer = new FileWriter(path);
////		BufferedWriter bufferedWriter = new BufferedWriter(writer);
////
////		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(formsEngine, query);
////		int count = 0;
////		while(manager.hasNext()) {
////			Object[] row = manager.next().getValues();
////			StringBuilder sb = new StringBuilder();
////			sb.append(row[0]).append("\t").append(row[1].toString().replaceAll("\t", "_").replaceAll("\n", "_").replaceAll("\r", "_")).append(LINE_SEPARATOR);
////			bufferedWriter.write(sb.toString());
////			count++;
////		}
////		bufferedWriter.close();
////		writer.close();
////
////		System.out.println("COUNT ::: " + count);
////		System.out.println("DONE");
//		
//		// 1) remove description
//		String query = "SELECT DISTINCT ?system ?description WHERE {"
//				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
//				+ "{?system <http://semoss.org/ontologies/Relation/Contains/Description> ?description}"
//				+ "}";
//		removeExistingProperties(formsEngine, query, "http://semoss.org/ontologies/Relation/Contains/Description");
//
//		// 2) remove poc
//		query = "SELECT DISTINCT ?system ?poc WHERE {"
//				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
//				+ "{?system <http://semoss.org/ontologies/Relation/Contains/POC> ?poc}"
//				+ "}";
//		removeExistingProperties(formsEngine, query, "http://semoss.org/ontologies/Relation/Contains/POC");
//		
//		// 3) remove ato
//		query = "SELECT DISTINCT ?system ?ato WHERE {"
//				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
//				+ "{?system <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ato}"
//				+ "}";
//		removeExistingProperties(formsEngine, query, "http://semoss.org/ontologies/Relation/Contains/ATO_Date");
//		
//		// 4) remove availability actual
//		query = "SELECT DISTINCT ?system ?avail WHERE {"
//				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
//				+ "{?system <http://semoss.org/ontologies/Relation/Contains/AvailabilityActual> ?avail}"
//				+ "}";
//		removeExistingProperties(formsEngine, query, "http://semoss.org/ontologies/Relation/Contains/AvailabilityActual");
//		
//		// 5) remove full system name
//		query = "SELECT DISTINCT ?system ?fullName WHERE {"
//				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
//				+ "{?system <http://semoss.org/ontologies/Relation/Contains/Full_System_Name> ?fullName}"
//				+ "}";
//		removeExistingProperties(formsEngine, query, "http://semoss.org/ontologies/Relation/Contains/Full_System_Name");
//		
//		// add in the new data from the csv files
//		String start = "C:\\Users\\SEMOSS\\Desktop\\Props\\";
//		String[] fileLocations = new String[]{
//				start + "ATO_Date.csv",
//				start + "AvailabilityActual.csv",
//				start + "Full_System_Names.csv",
//				start + "POC.csv",
//				start + "Descriptions.csv"
//		};
//
//		String[] dataTypes = new String[]{
//				"DATE",
//				"NUMBER",
//				"STRING",
//				"STRING",
//				"STRING"
//		};
//
//		int size = fileLocations.length;
//		for(int i = 0; i < size; i++) {
//			processFile(formsEngine, fileLocations[i], dataTypes[i]);
//		}
//		
//		
//		formsEngine.commit();
//		
//		System.out.println("TEST DUPLICATE DESCRIPTIONS !!! ");
//		manager = WrapperManager.getInstance().getRawWrapper(formsEngine, testQ);
//		while(manager.hasNext()) {
//			Object[] values = manager.next().getValues();
//			if( ((Number) values[1]).intValue() > 1 ) {
//				System.out.println("SYSTEM ::: " + values[0]);
//			}
//		}
//		System.out.println("DONE TEST DUPLICATE DESCRIPTIONS !!! ");
//	}
//	
	private static void removeExistingProperties(IDatabaseEngine eng, String query, String pred) {
		final String subprefix = "http://health.mil/ontologies/Concept/System/";
		List<Object[]> triplesToRemove = new ArrayList<Object[]>();
		// get existing values
		IRawSelectWrapper manager = null;
		try {
			manager = WrapperManager.getInstance().getRawWrapper(eng, query);
			while(manager.hasNext()) {
				Object[] row = manager.next().getValues();
				String system = row[0].toString();
				Object obj = row[1];
				
				String sub = subprefix + system;
				triplesToRemove.add(new Object[]{sub, pred, obj, false});
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(manager != null) {
				try {
					manager.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		for(Object[] triple : triplesToRemove) {
			eng.doAction(ACTION_TYPE.REMOVE_STATEMENT, triple);
		}
	}

	
	private static void processFile(IDatabaseEngine eng, String fileLoc, String dataType) {
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(',');
		helper.parse(fileLoc);

		String[] headers = helper.getHeaders();
		
		final String subprefix = "http://health.mil/ontologies/Concept/System/";
		final String pred = "http://semoss.org/ontologies/Relation/Contains/" + headers[1];
		
		Object[] row = null;
		while((row = helper.getNextRow()) != null) {
			String system = row[0].toString();
			Object obj = row[1];

			if(dataType.equals("STRING")) {
				obj = Utility.cleanString(obj.toString(), false);
			} else if(dataType.equals("DATE")) {
				obj = Utility.getDateAsDateObj(obj.toString());
			} else if(dataType.equals("NUMBER")) {
				obj = Utility.getDouble(obj.toString());
			}
			
			String sub = subprefix + system;
			eng.doAction(ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, pred, obj, false});
		}
	}


}
