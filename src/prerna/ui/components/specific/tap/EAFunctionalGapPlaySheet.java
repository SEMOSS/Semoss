package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class EAFunctionalGapPlaySheet extends GridPlaySheet {
	private IEngine hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
	
	private final String getEHRDataQuery = "SELECT DISTINCT ?data WHERE {{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?system ?provide ?data}} UNION {{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}{?provide <http://semoss.org/ontologies/Relation/Contains/SOR> 'Yes'}{?system ?provide ?data}}}} BINDINGS ?system {(<http://health.mil/ontologies/Concept/System/AHLTA>)(<http://health.mil/ontologies/Concept/System/CHCS>)(<http://health.mil/ontologies/Concept/System/CIS-Essentris>)}";
	private final String getEHRBLUQuery = "SELECT DISTINCT ?blu WHERE {{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?system <http://semoss.org/ontologies/Relation/Provide> ?blu}} BINDINGS ?system {(<http://health.mil/ontologies/Concept/System/AHLTA>)(<http://health.mil/ontologies/Concept/System/CHCS>)(<http://health.mil/ontologies/Concept/System/CIS-Essentris>)}";
	private final String getDHMSMDataQuery = "SELECT DISTINCT ?data WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Consists> ?task}{?task ?needs ?data}}";
	private final String getDHMSMBLUQuery = "SELECT DISTINCT ?blu WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Consists> ?task}{?task ?needs ?blu}}";
	private final String getFGDataQuery = "SELECT DISTINCT ?activity ?data WHERE {{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?fError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?activity <http://semoss.org/ontologies/Relation/Assigned> ?fError}{?fError <http://semoss.org/ontologies/Relation/Needs> ?data}}";
	private final String getFGBLUQuery = "SELECT DISTINCT ?activity ?blu WHERE {{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?fError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?activity <http://semoss.org/ontologies/Relation/Assigned> ?fError}{?fError <http://semoss.org/ontologies/Relation/Needs> ?blu}}";
	private final String getActivityDataWeightQuery = "SELECT DISTINCT ?activity ?activityWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?activity <http://semoss.org/ontologies/Relation/Contains/Dataweight> ?activityWeight}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}}";
	private final String getActivityBLUWeightQuery = "SELECT DISTINCT ?activity ?activityWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?activity <http://semoss.org/ontologies/Relation/Contains/BLUweight> ?activityWeight}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}}";
	private final String getActivityDataQuery = "SELECT DISTINCT ?activity ?data ?dataWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?needs <http://semoss.org/ontologies/Relation/Contains/weight> ?dataWeight}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}{?activity ?needs ?data}}";
	private final String getActivityBLUQuery = "SELECT DISTINCT ?activity ?blu ?bluWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?needs <http://semoss.org/ontologies/Relation/Contains/weight> ?bluWeight}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess <http://semoss.org/ontologies/Relation/Consists> ?activity}{?activity ?needs ?blu}}";
	private final String getBusinessProcessQuery = "SELECT DISTINCT ?businessProcess ?activity ?activityWeight WHERE {{?dhmsm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>}{?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}{?businessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>}{?consists <http://semoss.org/ontologies/Relation/Contains/weight> ?activityWeight}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?capability}{?capability <http://semoss.org/ontologies/Relation/Supports> ?businessProcess}{?businessProcess ?consists ?activity}}";
	private final String getFCCQuery = "SELECT DISTINCT ?BusinessProcess ?FCC ?weight WHERE { {?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>;}{?PartOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/PartOf> ;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?PartOf <http://semoss.org/ontologies/Relation/Contains/PercentBilled> ?weight}{?FCC ?PartOf ?BusinessProcess ;} }";
	
	private ArrayList<String> ehrData;
	private ArrayList<String> ehrBLU;
	private ArrayList<String> dhmsmData;
	private ArrayList<String> dhmsmBLU;
	private HashMap<String, ArrayList<String>> fgData;
	private HashMap<String, ArrayList<String>> fgBLU;
	private HashMap<String, String> activityDataWeight;
	private HashMap<String, String> activityBLUWeight;
	private HashMap<String, ArrayList<String[]>> activityDataMap;
	private HashMap<String, ArrayList<String[]>> activityBLUMap;
	private HashMap<String, ArrayList<String[]>> bpActivities;
	private HashMap<String, ArrayList<String[]>> bpFCCs;
	private HashMap<String, Double> fccCosts;
	
	@Override
	public void createData() {
		ehrData = getStringList(getEHRDataQuery);
		ehrBLU = getStringList(getEHRBLUQuery);
		dhmsmData = getStringList(getDHMSMDataQuery);
		dhmsmBLU = getStringList(getDHMSMBLUQuery);
		fgData = getStringListMap(getFGDataQuery);
		fgBLU = getStringListMap(getFGBLUQuery);
		activityDataWeight = getActivityWeightMap(getActivityDataWeightQuery);
		activityBLUWeight = getActivityWeightMap(getActivityBLUWeightQuery);
		activityDataMap = getActivityMap(getActivityDataQuery);
		activityBLUMap = getActivityMap(getActivityBLUQuery);
		bpActivities = getActivityMap(getBusinessProcessQuery);
		bpFCCs = getActivityMap(getFCCQuery);
		fccCosts = fillFCCCostHash();
	}
	
	@Override
	public void runAnalytics() {
		list = new ArrayList<Object[]>();
		if (query.equals("Activity")) {
			names = new String[2];
			names[0] = "Activity";
			names[1] = "delta Efficiency";
		} else if (query.equals("Business Process")) {
			names = new String[4];
			names[0] = "Business Process";
			names[1] = "delta Efficiency";
			names[2] = "BP Cost";
			names[3] = "Efficiency Savings";
		}
		HashMap<String, Double> activityEfficiency = new HashMap<String, Double>();
		HashMap<String, Double> bpCosts = new HashMap<String, Double>();
		
		for (String activity : activityDataWeight.keySet()) {
			Double dataEfficiency = 0.0;
			dataEfficiency = getEffiencyScore(activityDataMap.get(activity), ehrData, fgData.get(activity), dhmsmData);
			dataEfficiency *= Double.parseDouble(activityDataWeight.get(activity));
			activityEfficiency.put(activity, dataEfficiency);
		}
		for (String activity : activityBLUWeight.keySet()) {
			Double bluEfficiency = 0.0;
			bluEfficiency = getEffiencyScore(activityBLUMap.get(activity), ehrBLU, fgBLU.get(activity), dhmsmBLU);
			bluEfficiency *= Double.parseDouble(activityBLUWeight.get(activity));
			if (activityEfficiency.containsKey(activity)) {
				Double dataEfficiency = activityEfficiency.get(activity);
				activityEfficiency.put(activity, dataEfficiency + bluEfficiency);
			} else {
				activityEfficiency.put(activity, bluEfficiency);
			}
		}
		if (query.equals("Activity")) {
			for (String activity : activityEfficiency.keySet()) {
				Object[] temp = { activity, activityEfficiency.get(activity) };
				list.add(temp);
			}
		}
		
		else if (query.equals("Business Process")) {
			for (String bp : bpFCCs.keySet()) {
				Double bpCost = 0.0;
				for (String[] bpFCC : bpFCCs.get(bp)) {
					if (fccCosts.containsKey(bpFCC[0])) {
						bpCost += Double.parseDouble(bpFCC[1]) * fccCosts.get(bpFCC[0]);
					}
				}
				bpCosts.put(bp, bpCost);
			}
			
			for (String businessProcess : bpActivities.keySet()) {
				Double bpEfficiency = 0.0;
				for (String[] bpActivity : bpActivities.get(businessProcess)) {
					if (activityEfficiency.keySet().contains(bpActivity[0])) {
						bpEfficiency += (Double.parseDouble(bpActivity[1]) * activityEfficiency.get(bpActivity[0]));
					}
				}
				Double bpCost = 0.0;
				if (bpCosts.containsKey(businessProcess))
					bpCost = bpCosts.get(businessProcess);
				Object[] temp = { businessProcess, bpEfficiency, bpCost, bpEfficiency * bpCost };
				list.add(temp);
			}
		}
	}
	
	public ArrayList<String> getStringList(String query) {
		ArrayList<String> finalList = new ArrayList<String>();
		SesameJenaSelectWrapper sjsw = Utility.processQuery(hrCore, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			finalList.add(sjss.getVar(values[0]).toString());
		}
		return finalList;
	}
	
	public HashMap<String, ArrayList<String>> getStringListMap(String query) {
		HashMap<String, ArrayList<String>> finalMap = new HashMap<String, ArrayList<String>>();
		SesameJenaSelectWrapper sjsw = Utility.processQuery(hrCore, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			ArrayList<String> temp;
			
			String key = sjss.getVar(values[0]).toString();
			if (!finalMap.containsKey(key)) {
				temp = new ArrayList<String>();
				finalMap.put(key, temp);
			}
			finalMap.get(key).add(sjss.getVar(values[1]).toString());
		}
		return finalMap;
	}
	
	public HashMap<String, String> getActivityWeightMap(String query) {
		HashMap<String, String> finalMap = new HashMap<String, String>();
		SesameJenaSelectWrapper sjsw = Utility.processQuery(hrCore, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String key = sjss.getVar(values[0]).toString();
			String value = sjss.getVar(values[1]).toString();
			finalMap.put(key, value);
		}
		return finalMap;
	}
	
	public HashMap<String, ArrayList<String[]>> getActivityMap(String query) {
		HashMap<String, ArrayList<String[]>> finalMap = new HashMap<String, ArrayList<String[]>>();
		SesameJenaSelectWrapper sjsw = Utility.processQuery(hrCore, query);
		String[] values = sjsw.getVariables();
		while (sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			
			String key = sjss.getVar(values[0]).toString();
			if (!finalMap.containsKey(key)) {
				finalMap.put(key, new ArrayList<String[]>());
			}
			String[] temp = { sjss.getVar(values[1]).toString(), sjss.getVar(values[2]).toString() };
			finalMap.get(key).add(temp);
		}
		return finalMap;
	}
	
	public double getEffiencyScore(ArrayList<String[]> objectList, ArrayList<String> ehrList, ArrayList<String> fgList, ArrayList<String> dhmsmList) {
		double unweightedEfficiency = 0.0;
		if (objectList != null) {
			for (int i = 0; i < objectList.size(); i++) {
				String[] data = objectList.get(i);
				boolean dataRemoved = false;
				if (ehrList.contains(data[0])) {
					objectList.remove(i);
					i--;
					dataRemoved = true;
				}
				if ((!dataRemoved) && fgList != null) {
					if (fgList.contains(data[0])) {
						objectList.remove(i);
						i--;
						dataRemoved = true;
					}
				}
				if (!dataRemoved) {
					if (!dhmsmList.contains(data[0])) {
						objectList.remove(i);
						i--;
						dataRemoved = true;
					}
				}
				if (!dataRemoved) {
					unweightedEfficiency += Double.parseDouble(data[1]);
				}
			}
		}
		return unweightedEfficiency;
	}
	
	/**
	 * This should provide you with the cost for each FCC.
	 **/
	
	private HashMap<String, Double> fillFCCCostHash() {
		HashMap<String, Double> fccCost = new HashMap<String, Double>();
		String siteEngineName = "TAP_Site_Data";
		String fccQuery = "SELECT DISTINCT ?FCC (SUM(?TotalCost) AS ?Cost) WHERE {{?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>} {?FCCMTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC-MTF>}{?FCCMTF <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost }{?FCC <http://semoss.org/ontologies/Relation/Has> ?FCCMTF}} GROUP BY ?FCC";
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp(siteEngineName);
		SesameJenaSelectWrapper siteWrapper = new SesameJenaSelectWrapper();
		if (siteEngine == null) {
			Utility.showError("The database \"TAP_Site_Data\" could not be found. Process unable to continue");
		}
		siteWrapper.setQuery(fccQuery);
		siteWrapper.setEngine(siteEngine);
		siteWrapper.executeQuery();
		
		// get the bindings from it
		String[] fccColNames = siteWrapper.getVariables();
		try {
			while (siteWrapper.hasNext()) {
				SesameJenaSelectStatement sjss = siteWrapper.next();
				String fcc = (String) sjss.getVar(fccColNames[0]);
				Object cost = sjss.getVar(fccColNames[1]);
				double doubleCost = 0.0;
				if (cost instanceof Double)
					doubleCost = (Double) cost;
				else if (cost instanceof Integer) {
					doubleCost = (Integer) cost * 1.0;
				}
				fccCost.put(fcc, doubleCost);
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		return fccCost;
	}
}
