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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import prerna.error.FileReaderException;
import prerna.util.Utility;

public class CSVMetamodelBuilder {

	private ArrayList<File> files;
	private Hashtable<String, Hashtable<String, LinkedHashSet<String>>> dataType = new Hashtable<String, Hashtable<String, LinkedHashSet<String>>>();
	private String[] header;
	private ArrayList<File> propFiles;
	private Hashtable<String, ArrayList<Hashtable<String, String[]>>> propFileData = new Hashtable<String, ArrayList<Hashtable<String, String[]>>>();

	private String[] keys = new String[]{"RELATION", "NODE_PROP"};
	private String[] tripleRelKeys = new String[]{"sub", "pred", "obj"};
	private String[] tripleNodePropKeys = new String[]{"sub", "prop"};

	public void setFiles(ArrayList<File> files) {
		this.files = files;
	}

	public void setPropFiles(ArrayList<File> propFiles) {
		this.propFiles = propFiles;
	}

	public Hashtable<String, ArrayList<Hashtable<String, String[]>>> returnPropFileDataResults() throws FileReaderException {
		if(propFiles != null)
		{
			File propFile = propFiles.get(0);
			Properties propDataProp = new Properties();
			FileInputStream fileIn = null;
			try {
				fileIn = new FileInputStream(propFile);
				propDataProp.load(fileIn);				
			} catch (FileNotFoundException e) {
				throw new FileReaderException("Could not find CSV PropFile: " + propFiles.get(0));
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileReaderException("Could not process CSV PropFile headers in " + propFiles.get(0));
			} finally {
				try {
					if(fileIn!=null)
						fileIn.close();
				}catch (IOException e) {
					e.printStackTrace();
				}
			}

			initaiatePropFileData();

			String relationList = propDataProp.getProperty(keys[0]).toString();
			String[] relationListSplit = relationList.split(";");
			for(Integer relIdx = 0; relIdx < relationListSplit.length; relIdx++) 
			{
				String[] tripleParts = relationListSplit[relIdx].split("@");
				Hashtable<String, String[]> innerHash = new Hashtable<String, String[]>();
				if(tripleParts.length != 3) {
					continue;
				} else {
					for(int tripleIdx = 0; tripleIdx < 3; tripleIdx++) 
					{
						String triplePart = tripleParts[tripleIdx];
						String[] value;
						if(triplePart.contains("+")) {
							value = triplePart.split("\\+");
						} else {
							value = new String[]{triplePart};
						}
						innerHash.put(tripleRelKeys[tripleIdx], value);
					}
				}
				ArrayList<Hashtable<String, String[]>> currRelHash = propFileData.get(keys[0]);
				currRelHash.add(innerHash);
			}

			String nodePropList = propDataProp.getProperty(keys[1]).toString();
			String [] nodePropListSplit = nodePropList.split(";");
			for(int nodePropIdx = 0; nodePropIdx < nodePropListSplit.length; nodePropIdx++)
			{
				String[] tripleParts = nodePropListSplit[nodePropIdx].split("%");
				String[] subject = null;
				for(int idx = 0; idx < tripleParts.length; idx++)
				{
					Hashtable<String, String[]> innerHash = new Hashtable<String, String[]>();
					String[] value;
					if(idx == 0) {
						String triplePart = tripleParts[idx];
						if(triplePart.contains("+")) {
							subject = triplePart.split("\\+");
						} else {
							subject = new String[]{triplePart};
						}
					} else {
						innerHash.put(tripleNodePropKeys[0], subject);
						String triplePart = tripleParts[idx];
						if(triplePart.contains("+")) {
							value = triplePart.split("\\+");
						} else {
							value = new String[]{triplePart};
						}
						innerHash.put(tripleNodePropKeys[1], value);	
						ArrayList<Hashtable<String, String[]>> currNodePropHash = propFileData.get(keys[1]);
						currNodePropHash.add(innerHash);
					}
				}
			}
		}
		return propFileData;
	}

	public Hashtable<String, Hashtable<String, LinkedHashSet<String>>> returnDataTypes() throws FileReaderException
	{
		//TODO: loop through multiple files?

		if(files != null)
		{
			File fileName = files.get(0);
			CsvListReader listReader = null;
			FileReader fileRead = null;
			try {
				fileRead = new FileReader(fileName);
				listReader = new CsvListReader(fileRead, CsvPreference.STANDARD_PREFERENCE);
				this.header = listReader.getHeader(true);

				initiateDataTypeHash();
				getAllDataType(listReader);
				getAllowedDataType();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				throw new FileReaderException("Could not find CSV file: " + files.get(0));
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileReaderException("Could not process CSV file headers in " + files.get(0));
			}finally{
				try{
					if(fileRead!=null)
						fileRead.close();
				}catch(IOException e) {
					e.printStackTrace();
				}
			}
			return dataType;
		}

		return dataType;
	}

	private void initiateDataTypeHash() {
		LinkedHashSet<String> headerSet = new LinkedHashSet<String>();
		for(int i = 0; i < header.length; i++)
		{
			headerSet.add(header[i]);
			if(header[i] != null) 
			{
				Hashtable<String, LinkedHashSet<String>> innerHash = new Hashtable<String, LinkedHashSet<String>>();
				innerHash.put("AllDataTypes", new LinkedHashSet<String>());
				innerHash.put("AllowedDataTypes", new LinkedHashSet<String>());
				dataType.put(header[i], innerHash);
			}
		}

		Hashtable<String, LinkedHashSet<String>> headerHash = new Hashtable<String, LinkedHashSet<String>>();
		headerHash.put("AllHeaders", headerSet);
		dataType.put("AllHeaders", headerHash);
	}

	private void initaiatePropFileData() {
		for(int i = 0; i < 2; i++) {
			ArrayList<Hashtable<String, String[]>> innerListHash = new ArrayList<Hashtable<String, String[]>>();
			propFileData.put(keys[i], innerListHash);
		}
	}

	private void getAllDataType(CsvListReader listReader) throws FileReaderException {
		List<String> instances;
		try {
			while((instances = listReader.read()) != null)
			{
				for(int i = 0; i < header.length; i++)
				{
					if(instances.get(i) != null)
					{
						String type = new String(Utility.processType(instances.get(i)));
						//TODO: have integer as separate load than double
						if(type.equals("INTEGER")) {
							type = "DOUBLE";
						}
						dataType.get(header[i]).get("AllDataTypes").add(type);
						dataType.get(header[i]).get("AllowedDataTypes").add(type);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileReaderException("Error processing data in CSV");
		}
	}

	//This method determines what data types are available in the dropdown for each property
	private void getAllowedDataType() {
		for(int i = 0; i < header.length; i++)
		{
			if(header[i] != null){
				Hashtable<String, LinkedHashSet<String>> headerObj = dataType.get(header[i]);
				LinkedHashSet<String> allowedTypes = headerObj.get("AllowedDataTypes");

				if(allowedTypes.isEmpty()) // it should never come in here, but as default, String is the only thing available
				{
					allowedTypes.add("STRING");
				}
				else if(allowedTypes.size() == 1) // if all cells of that column are the same type, you can load as that specific type, or string
				{
					allowedTypes.add("STRING");
				}
				else if(allowedTypes.size() >= 2) // if cell of that column are two different types
				{
					allowedTypes.clear();
					allowedTypes.add("STRING");
				}
			}

			//			HashSet<String> numSet = new HashSet<String>();
			//			numSet.add("DOUBLE");
			//			numSet.add("INTEGER");
			//			if(allowedTypes.isEmpty()) // it should never come in here, but as default, String is the only thing available
			//			{
			//				allowedTypes.add("STRING");
			//			}
			//			else if(allowedTypes.size() == 1) // if all cells of that column are the same type, you can load as that specific type, or string
			//			{
			//				allowedTypes.add("STRING");
			//			}
			//			else if(allowedTypes.size() == 2) // if cell of that column are two different types
			//			{
			//				if(allowedTypes.equals(numSet)) // if the two options are double and int, int is not an option as doubles cannot be cast to int
			//				{
			//					allowedTypes.remove("INTEGER");
			//					allowedTypes.add("STRING");
			//				}
			//				else // otherwise only string is available as anything else would have cast exception
			//				{
			//					allowedTypes.clear();
			//					allowedTypes.add("STRING");
			//				}
			//			}
			//			else if(allowedTypes.size() > 2) // if more than two types, only string is available
			//			{
			//				Iterator<String> typeIt = allowedTypes.iterator();
			//				while(typeIt.hasNext())
			//				{
			//					typeIt.next();
			//					typeIt.remove();
			//				}
			//				allowedTypes.add("STRING");
			//			}
		}
	}
}
