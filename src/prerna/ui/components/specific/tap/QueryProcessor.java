/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class QueryProcessor {
	
	public static String[] getNames(String query, String engineName) {
		String[] temp = null;
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			temp = values;
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return temp;
	}
	
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
	
	public static ArrayList<String> getRawStringList(String query, String engineName) {
		ArrayList<String> finalList = new ArrayList<String>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				finalList.add(sjss.getRawVar(values[0]).toString());
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
	
	/**
	 * Processes query so that first column is key, while second and third columns are placed into String array
	 * 
	 * @param query
	 * @param engineName
	 * @return
	 */
	public static HashMap<String, ArrayList<String[]>> getStringTwoArrayListMap(String query, String engineName) {
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
	
	public static HashMap<String, String[]> getStringTwoParameterMap(String query, String engineName) {
		HashMap<String, String[]> finalMap = new HashMap<String, String[]>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String key = sjss.getVar(values[0]).toString();
				String[] temp = { sjss.getVar(values[1]).toString(), sjss.getVar(values[2]).toString() };
				finalMap.put(key, temp);
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}
	
	/**
	 * Processes query that returns one numerical value
	 * 
	 * @param query
	 * @param engine
	 * @return
	 */
	public static Double getSingleCount(String query, String engineName) {
		Double total = 0.0;
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			ISelectStatement sjss = sjsw.next();
			total = Double.parseDouble(sjss.getVar(values[0]).toString());
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return total;
	}
	
		/**
	 * Processes query so that first column is key to outer hashmap and second column is key to the inner hashmap.
	 *  The value of the inner hashmap is a double.
	 * 
	 * @param query
	 * @param engineName
	 * @return HashMap<String, HashMap<String, Double>> 
	 */
	public static HashMap<String, Double> getDoubleVector(String query, String engineName) 
	{
		HashMap<String, Double> finalMap = new HashMap<String, Double>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String oneKey = sjss.getVar(values[0]).toString();
				finalMap.put(oneKey, Double.parseDouble(sjss.getVar(values[1]).toString()));
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}

}
