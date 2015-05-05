package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.ui.components.playsheets.OCONUSMapPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * PlaySheet that displays geospatial map of data duplication. Used in TAP_Core_Data, Data-Perspective
 * 
 * @author kepark
 * 
 */
public class DataDuplicationMapPlaySheet extends OCONUSMapPlaySheet {
	// Query to create initial dataset of each dc site with its latitude and longitude. size is a place holder that will be filled later
	private final String getInitialQuery = "SELECT ?DCSite ?lat ?lon ?size WHERE { {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite> ;} {?DCSite  <http://semoss.org/ontologies/Relation/Contains/LONG> ?lon}{?DCSite  <http://semoss.org/ontologies/Relation/Contains/LAT> ?lat} }";
	// Queries that connect dc site to system, system to data object if Migration Reference (no duplication), and system to data object if Migration
	// Reference (with duplication)
	private final String getDCSiteSystemQuery = "SELECT DISTINCT ?DCSite ?System WHERE {{?DCSite a <http://semoss.org/ontologies/Concept/DCSite>}{?SystemDCSite a <http://semoss.org/ontologies/Concept/SystemDCSite>}{?System a <http://semoss.org/ontologies/Concept/System>}{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite}{?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite}}";
	private final String getSystemDataQuery = "SELECT DISTINCT ?System ?DataObject WHERE {{?System a <http://semoss.org/ontologies/Concept/System>}{?DataObject a <http://semoss.org/ontologies/Concept/DataObject>}{?DOS a <http://semoss.org/ontologies/Concept/DataObjectSource>}{?SourceType a <http://semoss.org/ontologies/Concept/SourceType>}{?System <http://semoss.org/ontologies/Relation/Designated> ?DOS}{?DOS <http://semoss.org/ontologies/Relation/LabeledAs> ?SourceType}{?DOS <http://semoss.org/ontologies/Relation/Delivers> ?DataObject} BIND( <http://health.mil/ontologies/Concept/SourceType/MigrationReference> AS ?SourceType)}";
	private final String getDataDuplicateQuery = "SELECT DISTINCT ?System ?DataObject WHERE {{?System a <http://semoss.org/ontologies/Concept/System>}{?System2 a <http://semoss.org/ontologies/Concept/System>}{?DOS2 a <http://semoss.org/ontologies/Concept/DataObjectSource>}{?DataObject a <http://semoss.org/ontologies/Concept/DataObject>}{?DOS a <http://semoss.org/ontologies/Concept/DataObjectSource>}{?SourceType a <http://semoss.org/ontologies/Concept/SourceType>}{?System <http://semoss.org/ontologies/Relation/Designated> ?DOS}{?System2 <http://semoss.org/ontologies/Relation/Designated> ?DOS2}{?DOS <http://semoss.org/ontologies/Relation/LabeledAs> ?SourceType}{?DOS <http://semoss.org/ontologies/Relation/Delivers> ?DataObject} {?DOS2 <http://semoss.org/ontologies/Relation/LabeledAs> ?SourceType}{?DOS2 <http://semoss.org/ontologies/Relation/Delivers> ?DataObject} BIND( <http://health.mil/ontologies/Concept/SourceType/MigrationReference> AS ?SourceType)}";
	
	/**
	 * Performs logic to fill in size that will be displayed on geospatial map
	 * 
	 * @return Hashtable in JSON format
	 */
	@Override
	public Hashtable processQueryData() {
		// Create engines that will be queried on
		IEngine tapCore = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		IEngine tapSite = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
		
		// Holds results from queries
		HashMap<String, ArrayList<String>> dcSiteSystemMap = getRawOCONListMap(getDCSiteSystemQuery, tapSite);
		HashMap<String, ArrayList<String>> systemDataMap = new HashMap<String, ArrayList<String>>();
		
		// HashMap that will hold dc site and size data
		HashMap<String, Integer> dcSiteDataSourceCountMap = new HashMap<String, Integer>();
		
		// For question: # of systems that are SOR at each site
		if (query.contains("Data Object Sources")) {
			systemDataMap = getRawOCONListMap(getSystemDataQuery, tapCore);
			
			// Counts number of systems per site that are Migration References for at least one data object
			for (String dcSite : dcSiteSystemMap.keySet()) {
				for (String system : dcSiteSystemMap.get(dcSite)) {
					if (systemDataMap.containsKey(system)) {
						if (!dcSiteDataSourceCountMap.containsKey(dcSite)) {
							dcSiteDataSourceCountMap.put(dcSite, 0);
						} else {
							Integer count = dcSiteDataSourceCountMap.get(dcSite);
							count++;
							dcSiteDataSourceCountMap.put(dcSite, count);
						}
					}
				}
			}
		}
		
		// For question: number of data objects with at least 2 Migration References
		if (query.contains("Duplicative Data Objects")) {
			systemDataMap = getRawOCONListMap(getDataDuplicateQuery, tapCore);
			
			for (String dcSite : dcSiteSystemMap.keySet()) {
				Set<String> tempDataSet = new HashSet<String>();
				for (String system : dcSiteSystemMap.get(dcSite)) {
					if (systemDataMap.containsKey(system)) {
						tempDataSet.addAll(systemDataMap.get(system));
					}
				}
				Integer count = tempDataSet.size();
				dcSiteDataSourceCountMap.put(dcSite, count);
			}
		}
		
		list = getInitialList(getInitialQuery, tapSite);
		data = new HashSet();
		// Get column headers
		String[] var = getVariableArray();
		
		// Adds size to Hashtable to be sent as JSON
		for (int i = 0; i < list.size(); i++) {
			LinkedHashMap elementHash = new LinkedHashMap();
			Object[] listElement = list.get(i);
			if (dcSiteDataSourceCountMap.get(listElement[0]) != null) {
				listElement[3] = dcSiteDataSourceCountMap.get(listElement[0]);
			} else {
				listElement[3] = 0;
			}
			String colName;
			for (int j = 0; j < var.length; j++) {
				colName = var[j];
				
				if (dcSiteDataSourceCountMap.get(listElement[0].toString()) != null) {
					Double size = (double) Math.round(dcSiteDataSourceCountMap.get(listElement[0].toString()));
					System.out.println(size);
					elementHash.put("size", size);
				} else {
					elementHash.put("size", 0);
				}
				if (j != 3) {
					elementHash.put(colName, listElement[j]);
				}
				
			}
			data.add(elementHash);
		}
		
		allHash = new Hashtable();
		allHash.put("dataSeries", data);
		
		allHash.put("lat", "lat");
		allHash.put("lon", "lon");
		allHash.put("size", "size");
		allHash.put("locationName", var[0]);
		
		return allHash;
	}
	
	/**
	 * Processes initial query (DCSite, Lat, Long, Size) and stores it into appropriate format.
	 * 
	 * @param query
	 *            String query to be run on database
	 * @param engine
	 *            Database engine
	 * @return ArrayList of Object Arrays representing each row from query
	 */
	private ArrayList<Object[]> getInitialList(String query, IEngine engine) {
		ArrayList<Object[]> finalList = new ArrayList<Object[]>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		// Get column headers
		names = sjsw.getVariables();
		// Iterate through each row until all data has been processed
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			// DCSite has to be raw due to OCONUSMapPlaySheet
			Object[] temp = { sjss.getRawVar(names[0]), sjss.getVar(names[1]), sjss.getVar(names[2]), sjss.getVar(names[3]) };
			finalList.add(temp);
		}
		return finalList;
	}
	
	/**
	 * Used to populate HashMaps with results from queries
	 * 
	 * @param query
	 *            String query that will be run on database
	 * @param engine
	 *            Database engine
	 * @return HashMap where key is String and value is ArrayList of Strings that map to the key
	 */
	HashMap<String, ArrayList<String>> getRawOCONListMap(String query, IEngine engine) {
		HashMap<String, ArrayList<String>> finalMap = new HashMap<String, ArrayList<String>>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		// Get column headers
		String[] values = sjsw.getVariables();
		// Iterate through each row of query
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			
			String key = sjss.getRawVar(values[0]).toString();
			if (!finalMap.containsKey(key)) {
				finalMap.put(key, new ArrayList<String>());
			}
			String value = sjss.getRawVar(values[1]).toString();
			if (!finalMap.get(key).contains(value)) {
				finalMap.get(key).add(value);
			}
		}
		return finalMap;
	}
}
