/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Enables a variable to be a set of predefined constants.
 * This class defines constants for all types of playsheets, which includes their names, source file location, and the hint for a SPARQL query associated with each.
 */

public class PlaySheetRDFMapBasedEnum {
	
	static Map<String, Map<String, String>> masterObject; // this is rdf map key -> object containing sheet name, class name, hint
	private static PlaySheetRDFMapBasedEnum instance;
	
	// for storing in master object
//	private final static String sheetName = "sheetName";
	private final static String sheetClass = "sheetClass";
	private final static String sheetHint = "sheetHint";
	private final static String sheetID = "sheetID";
	
	// for getting off of rdf map
	private final static String MAP_HINT = "_HINT";
	private final static String MAP_CLASS = "";
//	private final static String MAP_NAME = "_NAME";
	
	public static PlaySheetRDFMapBasedEnum getInstance(){
		if (instance == null){
			instance = new PlaySheetRDFMapBasedEnum();
		}
		return instance;
	}

	private PlaySheetRDFMapBasedEnum() {
		// protected
	}
	
	
	public void setData(String[] psIdsToLoad, Properties props) {
		masterObject = new HashMap<String, Map<String,String>>();
		// for each playsheet listed on the map
		// grab as much information as we can about it
		// save in our master object so we can reference 
		for(String id : psIdsToLoad){
			Map<String, String> psObj = new HashMap<String, String>();
			psObj.put(sheetID, id);
			
			String psHintKey = id + MAP_HINT;
			String psClassKey = id + MAP_CLASS;
//			String psNameKey = id + MAP_NAME;
			
			if(props.containsKey(psHintKey)){
				psObj.put(sheetHint, props.getProperty(psHintKey));
			}
			else{
				psObj.put(sheetHint, "No hint defined");
			}
			if(props.containsKey(psClassKey)){
				psObj.put(sheetClass, props.getProperty(psClassKey));
			}
			else{
				psObj.put(sheetClass, "No class defined");
			}
//			if(props.containsKey(psNameKey)){
//				psObj.put(sheetName, props.getProperty(psNameKey));
//			}
//			else{
//				psObj.put(sheetName, "No name defined");
//			}
			masterObject.put(id, psObj);
		}
	}
	
	public String getSheetClass(String psId){
		return masterObject.get(psId).get(sheetClass);
	}
	
	public static String getSheetName(String psId){
		return masterObject.get(psId).get(sheetID);
	}
	
	public String getSheetHint(String psId){
		return masterObject.get(psId).get(sheetHint);
	}
	
	public static ArrayList<String> getAllSheetNames(){
		ArrayList<String> list = new ArrayList<String>();
		for (Map<String, String> e : masterObject.values())
		{
			list.add(e.get(sheetID));
		}
		return list;
	}
	
	public static ArrayList<String> getAllSheetClasses(){
		ArrayList<String> list = new ArrayList<String>();
		for (Map<String, String> e : masterObject.values())
		{
			list.add(e.get(sheetClass));
		}
		return list;
	}
	
	public static String getClassFromName(String checkName){
		String match = "";
		for (Map<String, String> e : masterObject.values())
		{
			if(e.get(sheetID).equals(checkName)){
				match = e.get(sheetClass);
				break;
			}
		}
		return match;
	}
	
	public static String getHintFromName(String checkName){
		String match = "";
		for (Map<String, String> e : masterObject.values())
		{
			if(e.get(sheetID).equals(checkName)){
				match = e.get(sheetHint);
				break;
			}
		}
		return match;
	}
	
	public static String getNameFromClass(String checkClass){
		String match = "";
		for (Map<String, String> e : masterObject.values())
		{
			if(e.get(sheetClass).equals(checkClass)){
				match = e.get(sheetID);
				break;
			}
		}
		return match;
	}

	public static String getIdFromClass(String checkClass) {
		String match = "";
		for (Map<String, String> e : masterObject.values())
		{
			if(e.get(sheetClass).equals(checkClass)){
				match = e.get(sheetID);
				break;
			}
		}
		return match;
	}
}
