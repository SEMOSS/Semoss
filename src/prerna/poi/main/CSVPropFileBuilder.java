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
package prerna.poi.main;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.util.Utility;

public class CSVPropFileBuilder{

	/*
	 * This class is used to take the information passed from the user during the 
	 * metamodel phase of creating a new database and writing it into a prop file.
	 * 
	 * The prop file is passed into the proper readers to create the engine and is 
	 * written onto the server.  This will enable a user to reuse the prop file generated
	 * to load additional databases or add to the existing database assuming the desired 
	 * database metamodel is the same and the csv files are formatted the same.
	 */

	// string builder to contain the relationship string 
	private StringBuilder relationships = new StringBuilder();
	
	// string builder to contain the node_properties
	private StringBuilder node_properties = new StringBuilder();
	
	// map containing the data types for each column of the csv file
	private Hashtable<String, String> dataTypeHash = new Hashtable<String, String>();
	
	// prop hash containing the information of the prop file
	// passed into the readers to get schema metadata around the file to upload
	private Hashtable<String, String> propHash = new Hashtable<String, String>();

	// string containing the entire prop file
	// used to write the prop file to be reused for future database loading
	// this contains the same format as the propHash class variable
	private StringBuilder propFile = new StringBuilder();
	
	/**
	 * Used to add a property into the prop file
	 * @param sub					List containing the column headers to concatenate as the main node
	 * @param obj					List containing the column headers to concatenate as the property on the main node
	 * @param dataType				The data type of the property column.
	 */
	public void addProperty(List<String> sub, List<String> obj, String dataType) {
		// concatenate the subject column headers using a plus sign
		Iterator<String> subIt = sub.iterator();
		String subject = subIt.next();
		while(subIt.hasNext()){
			subject = subject + "+" + subIt.next();
		}
		
		// concatenate the object column headers using a plus sign
		Iterator<String> objIt = obj.iterator();
		String object = objIt.next();
		while(objIt.hasNext()){
			object = object + "+" + objIt.next();
		}
		
		// store the data type of the column 
		dataTypeHash.put(object, dataType);
		// add the node_property to the string builder
		// all the node property are combined in a string which is semicolon delimited
		// each individual property is the concatenation of the subject, a percent sign, and the object
		node_properties.append(subject+"%"+object+";");
	}

	/**
	 * Used to add a relationship into the prop file
	 * @param sub					List containing the column headers to concatenate as the upstream node of the relationship
	 * @param pred					String containing the verb describing the relationship between the subject and object
	 * @param obj					List containing the column headers to concatenate as the downstream node of the relationship
	 */
	public void addRelationship(List<String> sub, String pred, List<String> obj) {		
		// concatenate the subject column headers using a plus sign
		Iterator<String> subIt = sub.iterator();
		String subject = subIt.next();
		while(subIt.hasNext()){
			subject = subject + "+" + subIt.next();
		}

		// concatenate the object column headers using a plus sign
		Iterator<String> objIt = obj.iterator();
		String object = objIt.next();
		while(objIt.hasNext()){
			object = object + "+" + objIt.next();
		}

		// clean the predicate value such that it can be used and still form a valid URI
		// NOTE: this is only import for RDF databases... RDBMS database will not even use this
		pred = Utility.cleanString(pred, true).replaceAll("[()]", "").replaceAll(",", "");
		
		// add the relationship to the string builder
		// all the relationships are combined in a string which is semicolon delimited
		// each individual relationship is the concatenation of the subject, the at symbol, the predicate, the at symbol, and the object
		relationships.append(subject+"@"+pred+"@"+object+";");
	}
	
	/**
	 * TODO: there is a fundamental assumption in this code that concepts are always going to be loaded in as strings
	 * This dependency was added since URIs are always strings and upload used to be only for RDF.  This code only
	 * considers the data types for the columns passed in as properties.  These are the only columns stored within 
	 * the dataTypeHash.  If the column is not stored in the dataTypeHash, we automatically assume that each value is
	 * a string.
	 * 
	 * 
	 * Stores the data type for each column. Each column is referred to by its position in the csv and is assigned a data type value
	 * @param header				The input list of headers matching the header order from the file
	 */
	public void columnTypes(List<String> header){
		// input the total number of columns into the prop file and prop hash
		int numHeaders = header.size();
		propHash.put("NUM_COLUMNS", Integer.toString(numHeaders));
		propFile.append("NUM_COLUMNS" + "\t" + Integer.toString(numHeaders) + "\n");
		// loop through all the headers
		for(int i = 0; i < numHeaders; i++)
		{
			//TODO: this is where the assumption kicks in that if the column never appeared when we were adding node property
			// then the column is just a string
			
			// if the data type is defined for the column header
			if(header.get(i) != null && dataTypeHash.containsKey(header.get(i)))
			{
				// grab the data type and append it to the propFile and to the propHash
				propHash.put(Integer.toString(i+1), dataTypeHash.get(header.get(i)));
//				System.out.println(header.get(i) + ":" + Integer.toString(i+1) + ":" + dataTypeHash.get(header.get(i)));
				propFile.append(Integer.toString(i+1) + "\t" + dataTypeHash.get(header.get(i)) + "\n");
			}
			// else, the data type is not defined, so just load it as a string
			else
			{
				propHash.put(Integer.toString(i+1), "STRING");
//				System.out.println(header.get(i) + ":" + Integer.toString(i+1) + ": STRING");
				propFile.append(Integer.toString(i+1) + "\tSTRING\n");
			}
		}
	}

	/**
	 * TODO: name is not consistent with functionality... this also constructs the propFile!!!
	 * 
	 * Adds all the instance string builder objects and values into the prop file and the prop hash
	 * @param startRowNumAsString			The value for the start row to load in the file during the data loading process
	 * @param endRowNumAsString				The value for the end row to load in the file during the data loading process
	 */
	public void constructPropHash(String startRowNumAsString, String endRowNumAsString, Map<String, String> additionalMods){
		// just add everything into the prop hash
		propHash.put("START_ROW", startRowNumAsString);
		propHash.put("END_ROW", endRowNumAsString);
		propHash.put("NOT_OPTIONAL", ";");
		propHash.put("RELATION", relationships.toString());
		propHash.put("NODE_PROP", node_properties.toString());
		propHash.put("RELATION_PROP", "");
		
		// and do the same and add it in the prop file
		propFile.append("START_ROW\t" + startRowNumAsString + "\n");
		propFile.append("END_ROW\t" + endRowNumAsString + "\n");
		propFile.append("RELATION\t" + relationships.toString() + "\n");
		propFile.append("NODE_PROP\t" + node_properties.toString() + "\n");
		propFile.append("RELATION_PROP\t \n");
		
		for(String key : additionalMods.keySet()) {
			String val = additionalMods.get(key);
			propHash.put(key, val);
			propFile.append(key + "\t" + val + "\n");
		}
	}

	/**
	 * TODO: name is not consistent with functionality... this also constructs the propFile!!!
	 * 
	 * Finishes construction of the prop hash and prop file and returns it
	 * @param startRowNumAsString			The value for the start row to load in the file during the data loading process
	 * @param endRowNumAsString				The value for the end row to load in the file during the data loading process
	 * @param additionalMods				Map containing additional modifications to add into the prop file
	 * 										Example is renaming of column for RDF loading
	 * @return								The prop hash
	 */
	public Hashtable<String, String> getPropHash(String startRowNumAsString, String endRowNumAsString, Map<String, String> additionalMods) {
		if(additionalMods == null) {
			additionalMods = new HashMap<String, String>();
		}
		constructPropHash(startRowNumAsString, endRowNumAsString, additionalMods);
		return propHash;
	}

	/**
	 * Returns the string containing all the data to write as a prop file
	 * @return
	 */
	public String getPropFile() {
		return propFile.toString();
	}
	
	public Map<String, String> getDataTypeHash() {
		return this.dataTypeHash;
	}
}
