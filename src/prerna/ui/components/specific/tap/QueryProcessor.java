package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class QueryProcessor {
	public static ArrayList<String> getStringList(String query, String engineName) {
		ArrayList<String> finalList = new ArrayList<String>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				finalList.add(sjss.getVar(values[0]).toString());
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalList;
		
	}
	
	public static HashMap<String, ArrayList<String>> getStringListMap(String query, String engineName) {
		HashMap<String, ArrayList<String>> finalMap = new HashMap<String, ArrayList<String>>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				ArrayList<String> temp;
				
				String key = sjss.getVar(values[0]).toString();
				if (!finalMap.containsKey(key)) {
					temp = new ArrayList<String>();
					finalMap.put(key, temp);
				}
				finalMap.get(key).add(sjss.getVar(values[1]).toString());
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}
	
	public static HashMap<String, TreeSet<String>> getStringSetMap(String query, String engineName) {
		HashMap<String, TreeSet<String>> finalMap = new HashMap<String, TreeSet<String>>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				TreeSet<String> temp;
				
				String key = sjss.getVar(values[0]).toString();
				if (!finalMap.containsKey(key)) {
					temp = new TreeSet<String>();
					finalMap.put(key, temp);
				}
				finalMap.get(key).add(sjss.getVar(values[1]).toString());
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}
	
	public static HashMap<String, String> getStringMap(String query, String engineName) {
		HashMap<String, String> finalMap = new HashMap<String, String>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String key = sjss.getVar(values[0]).toString();
				String value = sjss.getVar(values[1]).toString();
				finalMap.put(key, value);
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}
	
	public static HashMap<String, ArrayList<String[]>> getStringListArrayMap(String query, String engineName) {
		HashMap<String, ArrayList<String[]>> finalMap = new HashMap<String, ArrayList<String[]>>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				
				String key = sjss.getVar(values[0]).toString();
				if (!finalMap.containsKey(key)) {
					finalMap.put(key, new ArrayList<String[]>());
				}
				String[] temp = { sjss.getVar(values[1]).toString(), sjss.getVar(values[2]).toString() };
				finalMap.get(key).add(temp);
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}
	
	public static HashMap<String, HashMap<String, Double>> getDoubleMap(String query, String engineName) {
		HashMap<String, HashMap<String, Double>> finalMap = new HashMap<String, HashMap<String, Double>>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				
				String oneKey = sjss.getVar(values[0]).toString();
				String twoKey = sjss.getVar(values[1]).toString();
				if (!finalMap.containsKey(oneKey)) {
					finalMap.put(oneKey, new HashMap<String, Double>());
				}
				finalMap.get(oneKey).put(twoKey, Double.parseDouble(sjss.getVar(values[2]).toString()));
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}
	
	public static HashMap<String, ArrayList<String[]>> getStringTwoArrayListMap(String query, String engineName) {
		HashMap<String, ArrayList<String[]>> finalMap = new HashMap<String, ArrayList<String[]>>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				
				String key = sjss.getVar(values[0]).toString();
				String valueOne = sjss.getVar(values[1]).toString();
				String valueTwo = sjss.getVar(values[2]).toString();
				if (!finalMap.containsKey(key)) {
					finalMap.put(key, new ArrayList<String[]>());
				}
				String[] temp = { valueOne, valueTwo };
				finalMap.get(key).add(temp);
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}
}
