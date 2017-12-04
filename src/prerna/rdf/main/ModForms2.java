package prerna.rdf.main;

import java.util.ArrayList;
import java.util.List;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ModForms2 {

	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper();

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Forms_TAP_Core_Data.smss";
		BigDataEngine formsEngine = new BigDataEngine();
		formsEngine.openDB(engineProp);
		formsEngine.setEngineName("Forms_TAP_Core_Data");
		DIHelper.getInstance().setLocalProperty("Forms_TAP_Core_Data", formsEngine);

		// 1) remove availability actual
		String query = "SELECT DISTINCT ?system ?avail WHERE {"
				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
				+ "{?system <http://semoss.org/ontologies/Relation/Contains/AvailabilityActual> ?avail}"
				+ "}";
		removeExistingProperties(formsEngine, query, "http://semoss.org/ontologies/Relation/Contains/AvailabilityActual");
		
		// 2) remove availability required
		query = "SELECT DISTINCT ?system ?avail WHERE {"
				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
				+ "{?system <http://semoss.org/ontologies/Relation/Contains/AvailabilityRequired> ?avail}"
				+ "}";
		removeExistingProperties(formsEngine, query, "http://semoss.org/ontologies/Relation/Contains/AvailabilityRequired");
		
		// add in the new data from the csv files
		String start = "C:\\Users\\SEMOSS\\Desktop\\Mod\\";
		String[] fileLocations = new String[]{
				start + "Availability Actual edits.csv",
				start + "Availability Required edits.csv",
		};

		String[] dataTypes = new String[]{
				"NUMBER",
				"NUMBER",
		};

		int size = fileLocations.length;
		for(int i = 0; i < size; i++) {
			processFile(formsEngine, fileLocations[i], dataTypes[i]);
		}
		
		// now remove the relationship
		query = "SELECT DISTINCT ?system ?relationship ?portfolio WHERE {"
				+ "{?system a <http://semoss.org/ontologies/Concept/System>}"
				+ "{?portfolio a <http://semoss.org/ontologies/Concept/Portfolio>}"
				+ "{?relationship ?anything <http://semoss.org/ontologies/Relation>}"
				+ "{?system ?relationship ?portfolio}"
				+ "}";
		removeExistingRelationship(formsEngine, query);

		fileLocations = new String[]{start + "Portfolio Board Mappings.csv"};
		String[] rels = new String[]{"BelongsTo"};
		size = fileLocations.length;
		for(int i = 0; i < size; i++) {
			processRelationFile(formsEngine, fileLocations[i], rels[i]);
		}
		
		formsEngine.commit();
		
		System.out.println("Done");
	}
	
	private static void processRelationFile(IEngine eng, String fileLoc, String relationship) {
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(',');
		helper.parse(fileLoc);

		String[] headers = helper.getHeaders();
		
		final String subprefix = "http://health.mil/ontologies/Concept/" + headers[0] + "/";
		final String objprefix = "http://health.mil/ontologies/Concept/" + headers[1] + "/";
		final String basePred = "http://semoss.org/ontologies/Relation";
		final String relPred = "http://semoss.org/ontologies/Relation/" + relationship;
		final String instancePred = "http://health.mil/ontologies/Relation/" + relationship + "/";
		
		Object[] row = null;
		while((row = helper.getNextRow()) != null) {
			String system = row[0].toString();
			String portfolio = row[1].toString();

			String sub = subprefix + system;
			String obj = objprefix + portfolio;
			eng.doAction(ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, basePred, obj, true});
			eng.doAction(ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, relPred, obj, true});
			eng.doAction(ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, instancePred +  system + ":" + portfolio, obj, true});
		}
	}

	private static void removeExistingRelationship(IEngine eng, String query) {
		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(eng, query);
		List<Object[]> triplesToRemove = new ArrayList<Object[]>();
		while(manager.hasNext()) {
			Object[] row = manager.next().getRawValues();
		
			triplesToRemove.add(new Object[]{row[0], row[1], row[2], true});
		}
		
		for(Object[] triple : triplesToRemove) {
			eng.doAction(ACTION_TYPE.REMOVE_STATEMENT, triple);
		}
	}

	private static void removeExistingProperties(IEngine eng, String query, String pred) {
		final String subprefix = "http://health.mil/ontologies/Concept/System/";
		// get existing values
		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(eng, query);
		List<Object[]> triplesToRemove = new ArrayList<Object[]>();
		while(manager.hasNext()) {
			Object[] row = manager.next().getValues();
			String system = row[0].toString();
			Object obj = row[1];
			
			String sub = subprefix + system;
			triplesToRemove.add(new Object[]{sub, pred, obj, false});
		}
		
		for(Object[] triple : triplesToRemove) {
			eng.doAction(ACTION_TYPE.REMOVE_STATEMENT, triple);
		}
	}

	
	private static void processFile(IEngine eng, String fileLoc, String dataType) {
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
