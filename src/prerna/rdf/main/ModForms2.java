package prerna.rdf.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.vocabulary.RDF;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.ACTION_TYPE;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ModForms2 {

//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadDIHelper();
//
//		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Forms_TAP_Core_Data.smss";
//		BigDataEngine formsEngine = new BigDataEngine();
//		formsEngine.open(engineProp);
//		formsEngine.setEngineId("Forms_TAP_Core_Data");
//		DIHelper.getInstance().setLocalProperty("Forms_TAP_Core_Data", formsEngine);
//
//		// 1) remove availability actual
//		String query = "SELECT DISTINCT ?system ?avail WHERE {"
//				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
//				+ "{?system <http://semoss.org/ontologies/Relation/Contains/AvailabilityActual> ?avail}"
//				+ "}";
//		removeExistingProperties(formsEngine, query, "http://semoss.org/ontologies/Relation/Contains/AvailabilityActual");
//		
//		// 2) remove availability required
//		query = "SELECT DISTINCT ?system ?avail WHERE {"
//				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
//				+ "{?system <http://semoss.org/ontologies/Relation/Contains/AvailabilityRequired> ?avail}"
//				+ "}";
//		removeExistingProperties(formsEngine, query, "http://semoss.org/ontologies/Relation/Contains/AvailabilityRequired");
//		
//		// add in the new data from the csv files
//		String start = "C:\\Users\\SEMOSS\\Desktop\\Mod\\";
//		String[] fileLocations = new String[]{
//				start + "Availability Actual edits.csv",
//				start + "Availability Required edits.csv",
//		};
//
//		String[] dataTypes = new String[]{
//				"NUMBER",
//				"NUMBER",
//		};
//
//		int size = fileLocations.length;
//		for(int i = 0; i < size; i++) {
//			processFile(formsEngine, fileLocations[i], dataTypes[i]);
//		}
//		
//		// now remove the relationship
//		query = "SELECT DISTINCT ?system ?relationship ?portfolio WHERE {"
//				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
//				+ "{?portfolio a <http://semoss.org/ontologies/Concept/Portfolio>}"
//				+ "{?relationship ?anything <http://semoss.org/ontologies/Relation>}"
//				+ "{?system ?relationship ?portfolio}"
//				+ "}";
//		removeExistingRelationship(formsEngine, query);
//
//		fileLocations = new String[]{start + "Portfolio Board Mappings.csv"};
//		String[] rels = new String[]{"BelongsTo"};
//		size = fileLocations.length;
//		for(int i = 0; i < size; i++) {
//			processRelationFile(formsEngine, fileLocations[i], rels[i]);
//		}
//		
//		formsEngine.commit();
//		
//		System.out.println("Done");
//	}
	
	private static void processRelationFile(IDatabaseEngine eng, String fileLoc, String relationship) {
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(',');
		helper.parse(fileLoc);

		String[] headers = helper.getHeaders();
		
		final String subprefix = "http://health.mil/ontologies/Concept/" + headers[0] + "/";
		final String objprefix = "http://health.mil/ontologies/Concept/" + headers[1] + "/";
		final String objType = "http://semoss.org/ontologies/Concept/" + headers[1];
		final String basePred = "http://semoss.org/ontologies/Relation";
		final String relPred = "http://semoss.org/ontologies/Relation/" + relationship;
		final String instancePred = "http://health.mil/ontologies/Relation/" + relationship + "/";
		
		Object[] row = null;
		while((row = helper.getNextRow()) != null) {
			String system = row[0].toString();
			String portfolio = Utility.cleanString(row[1].toString(), true);

			String sub = subprefix + system;
			String obj = objprefix + portfolio;
			eng.doAction(ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, basePred, obj, true});
			eng.doAction(ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, relPred, obj, true});
			eng.doAction(ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, instancePred +  system + ":" + portfolio, obj, true});
			
			eng.doAction(ACTION_TYPE.ADD_STATEMENT, new Object[]{obj, RDF.TYPE.toString(), objType, true});
		}
	}

	private static void removeExistingRelationship(IDatabaseEngine eng, String query) {
		List<Object[]> triplesToRemove = new ArrayList<Object[]>();

		IRawSelectWrapper manager = null;
		try {
			manager = WrapperManager.getInstance().getRawWrapper(eng, query);
			while(manager.hasNext()) {
				Object[] row = manager.next().getRawValues();
				triplesToRemove.add(new Object[]{row[0], row[1], row[2], true});
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
