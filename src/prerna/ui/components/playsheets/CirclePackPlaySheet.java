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

package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IHeadersDataRow;

/**
 * The Play Sheet for creating a CirclePack diagram using names and children.
 */
public class CirclePackPlaySheet extends BrowserPlaySheet {

	/**
	 * Constructor for CirclePackPlaySheet.
	 */
	public CirclePackPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(1200, 800));
		String workingDir = System.getProperty("user.dir");
		fileName = "file://" + workingDir
				+ "/html/MHS-RDFSemossCharts/app/circlepack.html";
	}

	/**
	 * Method processQueryData. Processes the data from the SPARQL query into an
	 * appropriate format for the specific play sheet. CirclePack query
	 * structure: ?level1 ?level2 ?level3 ?size; last column must be size to assign the size of the circles
	 * 
	 * @return Hashtable - Consists of all nodes in tree structure CirclePack.
	 */

	public void processQueryData() {
		Hashtable allHash = new Hashtable();

		Map<String, Map<String, Map>> rootMap = new HashMap<String, Map<String, Map>>();
		Map<String, Map> currentMap;

		// loop through the list
		Iterator<IHeadersDataRow> it = dataFrame.iterator();
		while(it.hasNext()) {
			Object[] listElements = it.next().getValues();

			// if there is no data, go to the next one in the list
			if (listElements == null || listElements.length == 0) {
				continue;
			}

			int j = 0;
			String currentValue = listElements[j].toString().replace("\"", "");
			// previous and current pointers all point to root
			currentMap = rootMap.get(currentValue); // assign for root

			if (currentMap == null) {
				currentMap = new HashMap<String, Map>();
				rootMap.put(currentValue, currentMap);
			}

			Map<String, Map> nextMap = currentMap;

			j++;
			for (; j < listElements.length; j++) {
				// get current value from array
				currentValue = listElements[j].toString().replace("\"", "");

				// get target key
				// first loop will be root, second loop will be level 2, 3 loop
				// => level 3
				nextMap = currentMap.get(currentValue);

				// new value
				if (nextMap == null) {
					nextMap = new HashMap<String, Map>();
					currentMap.put(currentValue, nextMap);
				}

				// current now becomes next,
				// nextMap will be reset again in the next iteration
				currentMap = nextMap;
			}
		}
		HashSet hashSet = new HashSet();
		
		processTree(rootMap, hashSet);
		//System.out.println(rootMap);
		//printHashSet(hashSet);
		String root = engine.getEngineId();

		allHash.put("name", root);
		allHash.put("children", hashSet);

		this.dataHash = allHash;
	}

	/*
	 * //this checks the data to make sure it is structured correctly public
	 * static void printHashSet(HashSet data){
	 * 
	 * if(data == null ){ return; } Iterator iter = data.iterator();
	 * while(iter.hasNext()){ Map t = (Map) iter.next(); if(t.get("name")!=
	 * null){ System.out.println(t.get("name"));
	 * printHashSet((HashSet)t.get("children")); } } }
	 */

	public static void processTree(Map myMap, HashSet hashSet) {
		Set<String> keyset = myMap.keySet();
		HashSet childSet = new HashSet();
		Object[] array = keyset.toArray();

		for(int i = 0; i<array.length; i++){
			String key = (String) array[i];
			Map childMap = (Map) myMap.get(key);
			
			Map dataMap = new HashMap();
			dataMap.put("name", key);
			
			// continues to call child map if there's more map
			if (!childMap.isEmpty()) {
				//this try/catch block will try to convert string to double; if it throws an exception, add an additional level;
				//this will assume that the numbers will only appear as the last column.
				try{
					Set <String> keyset2 = childMap.keySet();
					Object [] array2 = keyset2.toArray();
					double size = Double.parseDouble((String) array2[0]);
					dataMap.put("size", size);
					hashSet.add(dataMap);
				}catch(NumberFormatException e){
					dataMap.put("children", childSet);
					hashSet.add(dataMap);
				}
				
				//pass in the next level hashSet
				processTree(childMap, childSet); // recursive call.
				//empty the childSet to store children for next level
				childSet = new HashSet();
			}
		}
	}
}