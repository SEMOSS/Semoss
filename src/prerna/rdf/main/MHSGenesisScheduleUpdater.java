package prerna.rdf.main;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.hp.hpl.jena.vocabulary.RDF;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class MHSGenesisScheduleUpdater {

		
	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
		
		engineProp = "C:\\workspace\\Semoss_Dev\\db\\TAP_Site_Data.smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineId("TAP_Site_Data");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("TAP_Site_Data", coreEngine);
		
		// update site assignments
		updateSiteToWave(coreEngine);
		
		// update waves to year-quarter
		// first, delete all wave to year-quarter nodes
		deleteWaveToYearQuarter(coreEngine);
		// do the wave to year-quarter
		addWaveToYearQuarter(coreEngine);
		
		String q = "SELECT DISTINCT ?Wave ?StartDate ?EndDate WHERE { {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?StartDate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>} {?Wave <http://semoss.org/ontologies/Relation/BeginsOn> ?StartDate} {?EndDate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>} {?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?EndDate} }";
		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(coreEngine, q);
		while(it.hasNext()) {
			Object[] uriRow = it.next().getRawValues();
			System.out.println("This should return " + Arrays.toString(uriRow));
		}
		
		coreEngine.commit();
	}

	private static void updateSiteToWave(IEngine engine) {
		List<String> updateSites = new Vector<String>();
		updateSites.add("LEMOORE");
		updateSites.add("MOUNTAIN_HOME_AFB");
		updateSites.add("MONTEREY");
		updateSites.add("TRAVIS_AFB");
		
		List<String> newWaves = new Vector<String>();
		newWaves.add("1");
		newWaves.add("1");
		newWaves.add("1");
		newWaves.add("1");

		StringBuilder sb = new StringBuilder("bindings ?dcsite {");
		for(int i = 0; i < updateSites.size(); i++) {
			sb.append("(<http://health.mil/ontologies/Concept/DCSite/").append(updateSites.get(i)).append(">)");
		}
		sb.append("}");
		
		String q = "select distinct ?wave ?rel ?dcsite where {"
				+ "{?wave a <http://semoss.org/ontologies/Concept/Wave>} "
				+ "{?dcsite a <http://semoss.org/ontologies/Concept/DCSite>}"
				+ "{?wave ?rel ?dcsite} } " + sb.toString();
		
		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(engine, q);
		while(it.hasNext()) {
			Object[] uriRow = it.next().getRawValues();
			System.out.println("Delete wave to dc site " + Arrays.toString(uriRow));
			deleteRel(engine, uriRow);
		}
		
		// now add new waves for each one
		String relName = "Contains";
		for(int i = 0; i < updateSites.size(); i++) {
			String wave = newWaves.get(i);
			String dcSite = updateSites.get(i);
			
			Object[] rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Wave/" + wave;
			rel[1] = "http://health.mil/ontologies/Relation/" + relName + "/" + wave + ":" + dcSite;
			rel[2] = "http://health.mil/ontologies/Concept/DCSite/" + dcSite;
			
			System.out.println("Add wave to dc site " + Arrays.toString(rel));
			addRel(engine, rel);
			
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Wave/" + wave;
			rel[1] = "http://semoss.org/ontologies/Relation/" + relName;
			rel[2] = "http://health.mil/ontologies/Concept/DCSite/" + dcSite;
			
			System.out.println("Add wave to dc site " + Arrays.toString(rel));
			addRel(engine, rel);
			
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Wave/" + wave;
			rel[1] = "http://semoss.org/ontologies/Relation";
			rel[2] = "http://health.mil/ontologies/Concept/DCSite/" + dcSite;
			
			System.out.println("Add wave to dc site " + Arrays.toString(rel));
			addRel(engine, rel);
		}
	}

	private static void deleteWaveToYearQuarter(IEngine engine) {
		String q = "select distinct ?wave ?rel ?yearquarter where {"
				+ "{?wave a <http://semoss.org/ontologies/Concept/Wave>} "
				+ "{?yearquarter a <http://semoss.org/ontologies/Concept/Year-Quarter>}"
				+ "{?wave ?rel ?yearquarter} }";
		
		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(engine, q);
		while(it.hasNext()) {
			Object[] uriRow = it.next().getRawValues();
			System.out.println("Delete wave to year quarter " + Arrays.toString(uriRow));
			deleteRel(engine, uriRow);
		}
	}
	
	private static void addWaveToYearQuarter(IEngine coreEngine) {
		// add BeginsOn
		Map<String, String> addOn = new HashMap<String, String>();
		addOn.put("IOC", 	"Q4FY2016");
		addOn.put("1", 		"Q1FY2019");
		addOn.put("4", 		"Q3FY2019");
		addOn.put("5", 		"Q4FY2019");
		addOn.put("3", 		"Q2FY2019");
		addOn.put("2", 		"Q3FY2019");
		addOn.put("6", 		"Q1FY2020");
		addOn.put("8", 		"Q2FY2022");
		addOn.put("7", 		"Q4FY2021");
		addOn.put("12", 	"Q1FY2023");
		addOn.put("10", 	"Q4FY2022");
		addOn.put("9", 		"Q3FY2022");
		addOn.put("11", 	"Q4FY2022");
		addOn.put("17",		"Q2FY2021");
		addOn.put("13",		"Q3FY2020");
		addOn.put("14", 	"Q4FY2020");
		addOn.put("15", 	"Q2FY2024");
		addOn.put("18", 	"Q3FY2021");
		addOn.put("16", 	"Q1FY2021");
		addOn.put("19",		"Q2FY2023");
		addOn.put("20", 	"Q4FY2023");
		addOn.put("22", 	"Q1FY2024");
		addOn.put("21", 	"Q3FY2023");
		addOn.put("23", 	"Q2FY2024");
		processWaveYear(coreEngine, addOn, "BeginsOn");
		
		// add EndsOn
		Map<String, String> endOn = new HashMap<String, String>();
		endOn.put("IOC", 	"Q2FY2018");
		endOn.put("1", 		"Q3FY2019");
		endOn.put("4", 		"Q2FY2020");
		endOn.put("5", 		"Q3FY2020");
		endOn.put("3", 		"Q1FY2020");
		endOn.put("2", 		"Q2FY2020");
		endOn.put("6", 		"Q4FY2020");
		endOn.put("8", 		"Q2FY2023");
		endOn.put("7", 		"Q1FY2023");
		endOn.put("12", 	"Q1FY2024");
		endOn.put("10", 	"Q4FY2023");
		endOn.put("9", 		"Q3FY2023");
		endOn.put("11", 	"Q4FY2023");
		endOn.put("17",		"Q3FY2022");
		endOn.put("13",		"Q4FY2021");
		endOn.put("14", 	"Q1FY2022");
		endOn.put("15", 	"Q2FY2025");
		endOn.put("18", 	"Q4FY2022");
		endOn.put("16", 	"Q2FY2022");
		endOn.put("19",		"Q2FY2024");
		endOn.put("20", 	"Q3FY2024");
		endOn.put("22", 	"Q1FY2025");
		endOn.put("21", 	"Q4FY2022");
		endOn.put("23", 	"Q2FY2025");
		processWaveYear(coreEngine, endOn, "EndsOn");

	}
	
	private static void processWaveYear(IEngine engine, Map<String, String> waveToYearMap, String relName) {
		for(String wave : waveToYearMap.keySet()) {
			String yearQuarter = waveToYearMap.get(wave);
			
			// add the wave to the year quarter
			Object[] rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Wave/" + wave;
			rel[1] = "http://health.mil/ontologies/Relation/" + relName + "/" + wave + ":" + yearQuarter;
			rel[2] = "http://health.mil/ontologies/Concept/Year-Quarter/" + yearQuarter;
			
			System.out.println("Add wave to year quarter " + Arrays.toString(rel));
			addRel(engine, rel);
			
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Wave/" + wave;
			rel[1] = "http://semoss.org/ontologies/Relation/" + relName;
			rel[2] = "http://health.mil/ontologies/Concept/Year-Quarter/" + yearQuarter;
			
			System.out.println("Add wave to year quarter " + Arrays.toString(rel));
			addRel(engine, rel);
			
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Wave/" + wave;
			rel[1] = "http://semoss.org/ontologies/Relation";
			rel[2] = "http://health.mil/ontologies/Concept/Year-Quarter/" + yearQuarter;
			
			System.out.println("Add wave to year quarter " + Arrays.toString(rel));
			addRel(engine, rel);
			
			// add wave in case it doesn't exist as a concept
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Wave/" + wave;
			rel[1] = RDF.type.toString();
			rel[2] = "http://semoss.org/ontologies/Concept/Wave";

			System.out.println("Add wave to year quarter " + Arrays.toString(rel));
			addRel(engine, rel);
			
			// add year quater in case it doesn't exist as a concept
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Year-Quarter/" + yearQuarter;
			rel[1] = RDF.type.toString();
			rel[2] = "http://semoss.org/ontologies/Concept/Year-Quarter";
			
			System.out.println("Add wave to year quarter " + Arrays.toString(rel));
			addRel(engine, rel);
			
			// add surrounding stuff
			String[] split = yearQuarter.split("FY");
			String quarter = split[0].replace("Q", "");
			String year = split[1].replace("FY", "");
			
			// add year quarter to year
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Year-Quarter/" + yearQuarter;
			rel[1] = "http://health.mil/ontologies/Relation/has/" + yearQuarter + ":" + year;
			rel[2] = "http://health.mil/ontologies/Concept/Year/" + year;
			
			System.out.println("Add year quarter to year " + Arrays.toString(rel));
			addRel(engine, rel);
			
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Year-Quarter/" + yearQuarter;
			rel[1] = "http://semoss.org/ontologies/Relation/has";
			rel[2] = "http://health.mil/ontologies/Concept/Year/" + year;
			
			System.out.println("Add year quarter to year " + Arrays.toString(rel));
			addRel(engine, rel);

			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Year-Quarter/" + yearQuarter;
			rel[1] = "http://semoss.org/ontologies/Relation";
			rel[2] = "http://health.mil/ontologies/Concept/Year/" + year;
			
			System.out.println("Add year quarter to year " + Arrays.toString(rel));
			addRel(engine, rel);

			
			// add year in case it doesn't exist
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Year/" + year;
			rel[1] = RDF.type.toString();
			rel[2] = "http://semoss.org/ontologies/Concept/Year";
			
			System.out.println("Add year " + Arrays.toString(rel));
			addRel(engine, rel);
			
			// add year quarter to quarter
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Year-Quarter/" + yearQuarter;
			rel[1] = "http://health.mil/ontologies/Relation/has/" + yearQuarter + ":" + year;
			rel[2] = "http://health.mil/ontologies/Concept/Quarter/" + quarter;
			
			System.out.println("Add year quarter to year " + Arrays.toString(rel));
			addRel(engine, rel);
			
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Year-Quarter/" + yearQuarter;
			rel[1] = "http://semoss.org/ontologies/Relation/has";
			rel[2] = "http://health.mil/ontologies/Concept/Quarter/" + quarter;
			
			System.out.println("Add year quarter to year " + Arrays.toString(rel));
			addRel(engine, rel);

			
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Year-Quarter/" + yearQuarter;
			rel[1] = "http://semoss.org/ontologies/Relation";
			rel[2] = "http://health.mil/ontologies/Concept/Quarter/" + quarter;
			
			System.out.println("Add year quarter to year " + Arrays.toString(rel));
			addRel(engine, rel);

			
			// add quarter in case it doesn't exist
			rel = new Object[3];
			rel[0] = "http://health.mil/ontologies/Concept/Quarter/" + quarter;
			rel[1] = RDF.type.toString();
			rel[2] = "http://semoss.org/ontologies/Concept/Quarter";
			
			System.out.println("Add quarter " + Arrays.toString(rel));
			addRel(engine, rel);
		}
	}
	
	private static void deleteRel(IEngine engine, Object[] rel) {
		Object[] del = new Object[4];
		del[0] = rel[0].toString();
		del[1] = rel[1].toString();
		del[2] = rel[2].toString();
		del[3] = true;
		engine.doAction(ACTION_TYPE.REMOVE_STATEMENT, del);
	}
	
	private static void addRel(IEngine engine, Object[] rel) {
		Object[] del = new Object[4];
		del[0] = rel[0];
		del[1] = rel[1];
		del[2] = rel[2];
		del[3] = true;
		engine.doAction(ACTION_TYPE.ADD_STATEMENT, del);
	}
	
}
