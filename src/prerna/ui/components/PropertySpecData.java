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
package prerna.ui.components;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * This is the core class used to show which classes are data properties as opposed to object properties.
 */
public class PropertySpecData {

	// for every predicate
	// get the predicate class name upto the URI
	// show only those
	// additionally, allow the user to update and add if there are others the
	// user wants to add
	
	// Need to find a way to eliminate duplicates, if the super type etc is all set

	public String[] columnNames = { "Supertype", "Subtype", "Concept", "Predicate", "Property" };

	public String[] columnNames2 = { "Predicate", "Relation", "Property" };

	public Object[][] dataList = null; // concept hierarchy list
	public Object[][] dataList2 = null; // predicate list

	public String subject2bRemoved = "";
	public String subject2bAdded = "";
	public String pred2bRemoved = "";
	public String pred2bAdded = "";
	public String prop2bRemoved = "";
	public String prop2bAdded = "";

	int conceptSize = 0;
	int predSize = 0;
	int propertySize = 0;

	Hashtable<String, String> objectHash = new Hashtable<String, String>();
	Hashtable<String, String> conceptAvailableHash = new Hashtable<String, String>();
	Hashtable<String, String> predicateAvailableHash = new Hashtable<String, String>();
	Hashtable<String, String> propertyAvailableHash = new Hashtable<String, String>();

	// this is to check I am not re-adding nodes that have been already added
	String stringofEverything = "";

	Hashtable<String, String> conceptHash = new Hashtable<String, String>();
	Hashtable<String, String> predicateHash = new Hashtable<String, String>();
	Hashtable<String, String> propertyHash = new Hashtable<String, String>();

	static final Logger logger = LogManager.getLogger(PropertySpecData.class.getName());

	/**
	 * Adds predicates to the hashtable.
	 * @param predicate 	Predicated to be added.
	 */
	public void addPredicate(String predicate) {
		// String className = Utility.getQualifiedClassName(predicate);
		logger.debug("Adding predicate " + predicate);
		if (!predicateAvailableHash.containsKey(predicate))
			predicateAvailableHash.put(predicate, predicate);
	}

	/**
	 * Adds concepts to the concept hashtable.
	 * If the existing hashtable contains the parent string, get the vector from the hashtable.
	 * If not, create a new child vector and put it in the concept hashtable.
	 * @param parent 	Parent string.
	 * @param child 	Child string.
	 */
	public void addConcept(String parent, String child) {
		String childVector = "";
		if(stringofEverything.indexOf(child) < 0 )
		{
			if (conceptHash.containsKey(parent)) {
				childVector = conceptHash.get(parent);
			}
			if (!childVector.contains(child)) {
				childVector = childVector + "@@" + child;
				conceptHash.put(parent, childVector);
				conceptSize++;
			}
			stringofEverything = stringofEverything + " " + parent + " " + child;
		}
	}

	/**
	 * Add available concept to the hashtable.
	 * @param concept 	Concept to be added, in string form.
	 */
	public void addConceptAvailable(String concept) {
		//addConcept(concept, concept);
		conceptAvailableHash.put(concept, concept);
		subject2bAdded = subject2bAdded + ";" + concept + "@@" + concept;
	}

	/**
	 * Add available predicate to the hashtable.
	 * @param predicate 	Predicate to be added, in string form.
	 */
	public void addPredicateAvailable(String predicate) {
		logger.debug("Adding predicate " + predicate);
		//addPredicate2(predicate, predicate);
		if (!predicateAvailableHash.containsKey(predicate))
			predicateAvailableHash.put(predicate, predicate);
		pred2bAdded = pred2bAdded + ";" + predicate + "@@" + predicate;
	}

	/**
	 * Add available property to the hashtable if it does not already contain the predicate as a key.
	 * @param predicate 	Predicate to be added, in string form.
	 */
	public void addPropertyAvailable(String predicate) {
		logger.debug("Adding predicate " + predicate);
		if (!propertyAvailableHash.containsKey(predicate))
			propertyAvailableHash.put(predicate, predicate);

		//propertyAvailableHash.put(predicate, predicate);
	}

	/**
	 * Adds concepts to the concept hashtable.
	 * @param parent 		Parent string, serves as the hashtable key.
	 * @param predicateName Predicate name.
	 */
	public void addPredicate2(String parent, String predicateName) {
		addConcept(parent, predicateName);
		
	/*	if(stringofEverything.indexOf(predicateName) < 0 )
		{
			String childVector = "";
			if (predicateHash.containsKey(parent)) {
				childVector = predicateHash.get(parent);
			}
			if (!childVector.contains(predicateName)) {
				childVector = childVector + "@@" + predicateName;
				predicateHash.put(parent, childVector);
				predSize++;
			}
			stringofEverything = stringofEverything + " " + parent + " " + predicateName;
		}*/
	}
	
	/**
	 * Adds concepts to the concept hashtable.
	 * @param parent 		Parent string, serves as the hashtable key.
	 * @param predicateName Predicate name.
	 */
	public void addProperty(String parent, String predicateName) {
		addConcept(parent, predicateName);
		/*if(stringofEverything.indexOf(predicateName) < 0 )
		{
			String childVector = "";
			if (propertyHash.containsKey(parent)) {
				childVector = propertyHash.get(parent);
			}
			if (!childVector.contains(predicateName)) {
				childVector = childVector + "@@" + predicateName;
				propertyHash.put(parent, childVector);
				propertySize++;
			}
			stringofEverything = stringofEverything + " " + parent + " " + predicateName;
		}*/
		addPropertyAvailable(predicateName);
	}

	/**
	 * This method will read through the predicate hash and generate the data lists.
	 * It shows the boolean, concept hierarchy, predicate, concept, and concept boolean.
	 */
	public void genPredList() {
		ArrayList<String> keyArray = new ArrayList<String>();
		keyArray.addAll(conceptHash.keySet());
//		keyArray.addAll(predicateHash.keySet());
//		keyArray.addAll(propertyHash.keySet());
		// shows the boolean and predicate
		// shows the boolean, the concept hierarchy, concept, concept bool,
		// predicate bool, property bool, parent
		dataList = new Object[conceptHash.size() + conceptSize + predicateHash.size() + predSize + propertyHash.size() + propertySize][6];
		// shows the predicate, relation boolean, property boolean
		dataList2 = new Object[predicateHash.size() + propertyHash.size()][3];
		int conceptIndex = -1;
		// generate the concept list first
		for (int keyIdx = 0; keyIdx < keyArray.size(); keyIdx++) {
			String key = keyArray.get(keyIdx);
			conceptIndex++;
			dataList[conceptIndex][2] = new Boolean(false);
			dataList[conceptIndex][3] = new Boolean(false); // not a predicate
			dataList[conceptIndex][4] = new Boolean(false); // not a property
			if (conceptAvailableHash.containsKey(key)) {
				logger.debug("Found the key " + key);
				dataList[conceptIndex][2] = new Boolean(true);
			}
			dataList[conceptIndex][0] = key;
			dataList[conceptIndex][1] = "Select All";
			dataList[conceptIndex][5] = key;
			String childString = conceptHash.get(key);
			StringTokenizer tokens = new StringTokenizer(childString, "@@");
			int parentIndex = conceptIndex;
			// because of the contains is type, I cannot see if they are property
			boolean concept = true;
			boolean predicate = true;
			boolean property = true;
			while (tokens.hasMoreTokens()) {
				String child = tokens.nextToken();
				conceptIndex++;

				dataList[conceptIndex][2] = new Boolean(
						conceptAvailableHash.containsKey(child));
				dataList[conceptIndex][1] = child;
				dataList[conceptIndex][5] = key;
				dataList[conceptIndex][3] = new Boolean(
						predicateAvailableHash.containsKey(child)); // not a
																// predicate
				dataList[conceptIndex][4] = new Boolean(
						propertyAvailableHash.containsKey(child)); // not a
																// property
				concept = concept & conceptAvailableHash.containsKey(child);
				predicate = predicate & predicateAvailableHash.containsKey(child);
				property = property & propertyAvailableHash.containsKey(child);				
			}
			dataList[parentIndex][2] = new Boolean(concept);
			dataList[parentIndex][3] = new Boolean(predicate);
			dataList[parentIndex][4] = new Boolean(property);
		}

		// generate the predicate list next
		/*keys = predicateHash.keys();
		for (; keys.hasMoreElements(); conceptIndex++) {
			String key = keys.nextElement();
			dataList[conceptIndex][2] = new Boolean(false);
			dataList[conceptIndex][3] = new Boolean(false); // not a predicate
			dataList[conceptIndex][4] = new Boolean(false); // not a property
			if (predicateAvailableHash.containsKey(key)) {
				logger.debug("Found the key " + key);
				dataList[conceptIndex][3] = new Boolean(true); // predicate
																// block
			}
			dataList[conceptIndex][0] = key;
			dataList[conceptIndex][1] = "Select All";
			dataList[conceptIndex][5] = key;
			String childString = predicateHash.get(key);
			StringTokenizer tokens = new StringTokenizer(childString, "@@");
			while (tokens.hasMoreTokens()) {
				String child = tokens.nextToken();
				conceptIndex++;
				dataList[conceptIndex][3] = new Boolean(
						predicateAvailableHash.containsKey(child));
				dataList[conceptIndex][1] = child;
				dataList[conceptIndex][5] = key;
				dataList[conceptIndex][2] = new Boolean(false); // not a
																// predicate
				dataList[conceptIndex][4] = new Boolean(false); // not a
																// property
			}
		}

		// and the properties
		// generate the predicate list next
		keys = propertyHash.keys();
		for (; keys.hasMoreElements(); conceptIndex++) {
			String key = keys.nextElement();
			dataList[conceptIndex][2] = new Boolean(false);
			dataList[conceptIndex][3] = new Boolean(false); // not a predicate
			dataList[conceptIndex][4] = new Boolean(false); // not a property
			if (propertyAvailableHash.containsKey(key)) {
				logger.debug("Found the key " + key);
				dataList[conceptIndex][4] = new Boolean(true); // predicate
																// block
			}
			dataList[conceptIndex][0] = key;
			dataList[conceptIndex][1] = "Select All";
			dataList[conceptIndex][5] = key;
			String childString = propertyHash.get(key);
			StringTokenizer tokens = new StringTokenizer(childString, "@@");
			while (tokens.hasMoreTokens()) {
				String child = tokens.nextToken();
				conceptIndex++;
				dataList[conceptIndex][4] = new Boolean(
						propertyAvailableHash.containsKey(child));
				dataList[conceptIndex][1] = child;
				dataList[conceptIndex][5] = key;
				dataList[conceptIndex][2] = new Boolean(false); // not a
																// predicate
				dataList[conceptIndex][3] = new Boolean(false); // not a
																// property
			}
		}*/
	}

	/**
	 * Gets the cell value at a certain row and column index of the data list.
	 * @param row 		Row index.
	 * @param column 	Column index.
	
	 * @return Object 	Cell value. */
	public Object getValueAt(int row, int column) {
		return dataList[row][column];
	}

	/**
	 * Get the number of rows in the data list.
	
	 * @return int 	Number of rows. */
	public int getNumRows() {
		// use this call to convert the thing to array
		return dataList.length;
	}

	/**
	 * Sets the cell value at a certain row and column index.
	 * @param uriVal String
	 * @param value Object
	 * @param row int
	 * @param column int
	 */
	public void setValueAt(String uriVal, Object value, int row, int column) {
		// this will not only set the value here but also adjust the string
		// accordingly

		// if column is 1 then all the column 2 should be added
		String conceptParent = null;
		if (dataList[row][0] != null)
			conceptParent = dataList[row][0] + "";
		
		String knownConceptParent = dataList[row][5]+"";
		
		boolean concept = true;
		boolean predicate = false;
		boolean property = false;
		

		String childString = conceptHash.get(knownConceptParent);
		// if it is null then may be it is in predicateHash
		if(childString == null)
		{
//			concept = false;
			//childString = predicateHash.get(knownConceptParent);
			childString = conceptHash.get(knownConceptParent);
//			predicate = true;
		}
		if(childString == null)
		{
//			concept = false;
//			predicate = false;
//			property = true;
			//childString = propertyHash.get(knownConceptParent);
			childString = conceptHash.get(knownConceptParent);
		}
		
		// change of logic for concept vs. predicate etc.
		concept = (column == 2);
		predicate = (column == 3);
		property = (column == 4);
		
		
		if (conceptParent != null && conceptParent.length() >= 0) {
			// addToPropHash((Boolean)value, conceptParent);
			// add all of it
			selectRow(uriVal, value, row , column);
			if(childString != null)
			{
				int conceptIndex = 1;
				StringTokenizer tokens = new StringTokenizer(childString, "@@");
				while (tokens.hasMoreTokens()) {
					String child = tokens.nextToken();
					selectRow(uriVal, value, row + conceptIndex, column);
					addToString(conceptParent, child, (Boolean)value, concept, predicate, property, column);
					conceptIndex++;
					// addToPropHash((Boolean)value, child);
				}
			}
			//selectRow(conceptParent, value, row, column);
			//addToString(conceptParent, (Boolean)value, concept, predicate, property, column);
		} else
		{
			selectRow(uriVal, value, row, column);
			addToString(dataList[row][5]+"",dataList[row][1]+"", (Boolean)value, concept, predicate, property, column);
		}
	}
	
	/**
	 * Select a row of the data list.
	 * @param uriVal 	URI of the predicate.
	 * @param value 	Cell value.
	 * @param row 		Row index.
	 * @param column 	Column index.
	 */
	public void selectRow(String uriVal, Object value, int row, int column) {
		// this is of the form item, predicate boolean, property boolean
		// this is where the alternating piece kicks in

		if (column == 2)
		{
			dataList[row][3] = new Boolean(false);
			dataList[row][4] = new Boolean(false);
		}
		if (column == 3)
		{
			dataList[row][2] = new Boolean(false);
			dataList[row][4] = new Boolean(false);
		}
		if (column == 4)
		{
			dataList[row][2] = new Boolean(false);
			dataList[row][3] = new Boolean(false);
		}
		dataList[row][column] = (Boolean)value;
	}	
	
	/**
	 * Adds subject, predicates, and/or properties into the appropriate hashtables.
	 * @param parentEntity 	String containing the parent entity.
	 * @param entity 		Entity.
	 * @param add 			True if an entity needs to be added.
	 * @param concept 		True if an entity is a concept.
	 * @param predicate 	True if an entity is a predicate.
	 * @param property 		True if an entity is a property.	
	 */
	public void addToString(String parentEntity, String entity, boolean add, boolean concept, boolean predicate, boolean property, int column)
	{
		if(add)
		{
			if(concept)
			{
				this.subject2bRemoved = this.subject2bRemoved.replace(";"+parentEntity + "@@" + entity, "");
				this.pred2bAdded = this.pred2bAdded.replace(";"+ parentEntity + "@@" + entity, "");
				this.prop2bAdded = this.prop2bAdded.replace(";"+ parentEntity + "@@" + entity, "");
				//if(!conceptAvailableHash.containsKey(entity))
				this.subject2bAdded = this.subject2bAdded + ";" + entity + "@@" + entity;
				
				// removals
				this.pred2bRemoved = this.pred2bRemoved  + ";" + parentEntity + "@@" + entity;
				this.prop2bRemoved = this.pred2bAdded + ";" + parentEntity + "@@" + entity;

				conceptAvailableHash.put(entity, entity);
				predicateAvailableHash.remove(entity);
				propertyAvailableHash.remove(entity);
			}
			if(predicate)
			{
				this.pred2bRemoved = this.pred2bRemoved.replace(";"+parentEntity + "@@" + entity, "");
				this.prop2bAdded = this.prop2bAdded.replace(";"+parentEntity + "@@" + entity, "");
				this.subject2bAdded = this.subject2bAdded.replace(";" + parentEntity + "@@" + entity,"");
				//if(!predicateAvailableHash.containsKey(entity))
				this.pred2bAdded = this.pred2bAdded + ";" + parentEntity + "@@" + entity;
				
				// add it to removes
				this.subject2bRemoved = this.subject2bRemoved + ";" + parentEntity + "@@" + entity;
				this.prop2bRemoved = this.pred2bAdded + ";" + parentEntity + "@@" + entity;
				conceptAvailableHash.remove(entity);
				propertyAvailableHash.remove(entity);
				predicateAvailableHash.put(entity, entity);
			}
			if(property)
			{
				this.prop2bRemoved = this.prop2bRemoved.replace(";"+ parentEntity + "@@" + entity, "");
				this.pred2bAdded = this.pred2bAdded.replace(";"+ parentEntity + "@@" + entity, "");
				this.subject2bAdded = this.subject2bAdded.replace(";" + parentEntity + "@@" + entity,"");
				//if(!propertyAvailableHash.containsKey(entity))
				this.prop2bAdded = this.prop2bAdded + ";" + parentEntity + "@@" + entity;
				
				// removals
				this.subject2bRemoved = this.subject2bRemoved + ";" + parentEntity + "@@" + entity;
				this.pred2bRemoved = this.pred2bRemoved  + ";" + parentEntity + "@@" + entity;
				conceptAvailableHash.remove(entity);
				predicateAvailableHash.remove(entity);
				propertyAvailableHash.put(entity, entity);

			}
		}
		if(!add)
		{
			if(concept)
			{
				this.subject2bAdded = this.subject2bAdded.replace(";"+entity + "@@" + entity, "");
				this.pred2bRemoved = this.pred2bRemoved.replace(";"+parentEntity + "@@" + entity, "");
				this.prop2bRemoved = this.prop2bRemoved.replace(";"+parentEntity + "@@" + entity, "");
				//if(conceptAvailableHash.containsKey(entity))
					this.subject2bRemoved = this.subject2bRemoved + ";" + parentEntity + "@@" + entity;

				conceptAvailableHash.remove(entity);
				propertyAvailableHash.remove(entity);
				predicateAvailableHash.remove(entity);
			}
			if(predicate)
			{
				this.pred2bAdded = this.pred2bAdded.replace(";"+parentEntity + "@@" + entity, "");
				this.prop2bRemoved = this.prop2bRemoved.replace(";"+parentEntity + "@@" + entity, "");
				this.subject2bRemoved = this.subject2bRemoved.replace(";" + parentEntity + "@@" + entity,"");
				//if(predicateAvailableHash.containsKey(entity))
					this.pred2bRemoved = this.pred2bRemoved  + ";" + parentEntity + "@@" + entity;

				conceptAvailableHash.remove(entity);
				propertyAvailableHash.remove(entity);
				predicateAvailableHash.remove(entity);
			
			}
			if(property)
			{
				this.prop2bAdded = this.prop2bAdded.replace(";"+parentEntity + "@@" + entity, "");
				this.pred2bRemoved = this.pred2bRemoved.replace(";"+parentEntity + "@@" + entity, "");
				this.subject2bRemoved = this.subject2bRemoved.replace(";" + parentEntity + "@@" + entity,"");
				//if(propertyAvailableHash.containsKey(entity))
					this.prop2bRemoved = this.pred2bAdded + ";" + parentEntity + "@@" + entity;

				conceptAvailableHash.remove(entity);
				propertyAvailableHash.remove(entity);
				predicateAvailableHash.remove(entity);

			
			}
		}
		
		// printing what I have so far
		logger.warn("Additions " + subject2bAdded + "<>" + pred2bAdded + "<>" + prop2bAdded);
		logger.warn("Deletions " + subject2bRemoved + "<>" + pred2bRemoved + "<>" + prop2bRemoved);
		
	}
}
