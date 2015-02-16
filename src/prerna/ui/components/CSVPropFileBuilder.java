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
import java.util.Iterator;

public class CSVPropFileBuilder{

	private StringBuilder relationships = new StringBuilder();
	private StringBuilder node_properties = new StringBuilder();	
	
	private Hashtable<String, String> propHash = new Hashtable<String, String>();
	private Hashtable<String, String> dataTypeHash = new Hashtable<String, String>();
	
	private StringBuilder propFile = new StringBuilder();
	
	public CSVPropFileBuilder(){
		propFile.append("START_ROW\t2\n");
		propFile.append("END_ROW\t100000\n");
	}
	
	public void addProperty(ArrayList<String> sub, ArrayList<String> obj, String dataType) {
		Iterator<String> subIt = sub.iterator();
		String subject = subIt.next();
		while(subIt.hasNext()){
			subject = subject + "+" + subIt.next();
		}
		
		Iterator<String> objIt = obj.iterator();
		String object = objIt.next();
		while(objIt.hasNext()){
			object = object + "+" + objIt.next();
		}
		
		dataTypeHash.put(object, dataType);
		node_properties.append(subject+"%"+object+";");
	}

	public void addRelationship(ArrayList<String> sub, String pred, ArrayList<String> obj) {		
		Iterator<String> subIt = sub.iterator();
		String subject = subIt.next();
		while(subIt.hasNext()){
			subject = subject + "+" + subIt.next();
		}

		Iterator<String> objIt = obj.iterator();
		String object = objIt.next();
		while(objIt.hasNext()){
			object = object + "+" + objIt.next();
		}

		relationships.append(subject+"@"+pred+"@"+object+";");
	}

	public void columnTypes(ArrayList<String> header){
		propHash.put("NUM_COLUMNS", Integer.toString(header.size()));
		propFile.append("NUM_COLUMNS" + "\t" + Integer.toString(header.size()) + "\n");
		for(int i = 0; i < header.size(); i++)
		{
			if(header.get(i) != null && dataTypeHash.containsKey(header.get(i)))
			{
				propHash.put(Integer.toString(i+1), dataTypeHash.get(header.get(i)));
				System.out.println(header.get(i) + ":" + Integer.toString(i+1) + ":" + dataTypeHash.get(header.get(i)));
				propFile.append(Integer.toString(i+1) + "\t" + dataTypeHash.get(header.get(i)) + "\n");
			}
			else
			{
				propHash.put(Integer.toString(i+1), "STRING");
				System.out.println(header.get(i) + ":" + Integer.toString(i+1) + ": STRING");
				propFile.append(Integer.toString(i+1) + "\tSTRING\n");
			}
		}
	}

	public void constructPropHash(String startRowNumAsString, String endRowNumAsString){
		propHash.put("START_ROW", startRowNumAsString);
		propHash.put("END_ROW", endRowNumAsString);
		propHash.put("NOT_OPTIONAL", ";");
		propHash.put("RELATION", relationships.toString());
		propHash.put("NODE_PROP", node_properties.toString());
		propHash.put("RELATION_PROP", "");
		
		propFile.append("RELATION\t" + relationships.toString() + "\n");
		propFile.append("NODE_PROP\t" + node_properties.toString() + "\n");
		propFile.append("RELATION_PROP\t \n");
	}

	public Hashtable<String, String> getPropHash(String startRowNumAsString, String endRowNumAsString) {
		constructPropHash(startRowNumAsString, endRowNumAsString);
		return propHash;
	}
	
	public String getPropFile() {
		return propFile.toString();
	}
}
