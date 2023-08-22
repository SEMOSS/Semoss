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

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Create an excel file listing all the instance nodes and their properties for a type node specified on the DB Modification tab, Export Data section
 * Can export a maximum of 9 different node types, stored in different tabs, at one time
 */
public class NodeLoadingSheetWriter {

	private Boolean showSuccessMessage = true;
	private String writeFileName;
	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	/**
	 * Functions as the main method for the class
	 * Reorganizes the information from ExportNodeLoadSheetsListener into a format that is similar to a load sheet and saves it in a workbook
	 * The data is reorganized in a Hashtable<String, Vector<String[]>> where the keys become the sheet name and the instance data in the format Vector<String[]>  
	 * @param fileLoc 		String containing the path location to save the workbook
	 * @param hash 			Hashtable containing the information gotten from ExportNodeLoadSheetsListener, which gets the data from querying the engine
	 * @param engine
	 * @param nodeTypes
	 */
	public void writeNodeLoadingSheets(IDatabaseEngine engine, ArrayList<String> nodeTypes) {
		
		Hashtable<String, Vector<String[]>> hash = queryData(engine, nodeTypes);
		
		Hashtable<String, Vector<String[]>> preparedHash = prepareLoadingSheetExport(hash);
		
		//Create the blank excel file
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = DIR_SEPARATOR + "export" + DIR_SEPARATOR + "Nodes"+ DIR_SEPARATOR;
		if(writeFileName == null)
			writeFileName = "Nodes_LoadingSheet_" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "") + ".xlsx";
		String fileLoc = workingDir + folder + writeFileName;
		XSSFWorkbook wb = new XSSFWorkbook();

		//Write the loader sheet to excel
		XSSFSheet sheet = wb.createSheet("Loader");
		Vector<String[]> data = new Vector<String[]>();
		data.add(new String[]{"Sheet Name", "Type"});
		for(String key : hash.keySet()) {
			data.add(new String[]{key, "Usual"});
		}
		int count=0;
		for (int row=0; row<data.size();row++){
			XSSFRow row1 = sheet.createRow(count);
			count++;

			for (int col=0; col<data.get(row).length;col++){
				XSSFCell cell = row1.createCell(col);
				if(data.get(row)[col] != null) {
					cell.setCellValue(data.get(row)[col].replace("\"", ""));
				}
			}
		}

		//Add a sheet for each concept that lists all of its properties
		Set<String> keySet = preparedHash.keySet();
		for(String key: keySet){
			Vector<String[]> sheetVector = preparedHash.get(key);
			writeSheet(key, sheetVector, wb);
		}

		//Write the excel
		Utility.writeWorkbook(wb, fileLoc);
		if(showSuccessMessage)
			Utility.showMessage("Exported node properties successfully: " + fileLoc);
	}
	
	private Hashtable<String, Vector<String[]>> queryData(IDatabaseEngine engine, ArrayList<String> nodeTypes) {
		Hashtable<String, Vector<String[]>> hash = new Hashtable<String, Vector<String[]>>();
		for(String nodeType : nodeTypes) {
			ArrayList<Object[]> list = new ArrayList<Object[]>();
			String query = "SELECT ?s ?p ?prop WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
			query += nodeType;
			query += "> ;}OPTIONAL{ {?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;} {?s ?p ?prop ;} } }";


			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

			int count = 0;
			String[] names = wrapper.getVariables();
			HashSet<String> properties = new HashSet<String>();
			// now get the bindings and generate the data
			try {
				while(wrapper.hasNext()) {
					ISelectStatement sjss = wrapper.next();
					Object [] values = new Object[names.length];
					boolean filledData = true;

					for(int colIndex = 0;colIndex < names.length;colIndex++) {
						if(sjss.getVar(names[colIndex]) != null || sjss.getVar(names[colIndex]).toString().equals("")) {
							if(colIndex == 1 && !sjss.getVar(names[colIndex]).toString().equals("")) {
								properties.add((String) sjss.getVar(names[colIndex]));
							}
							values[colIndex] = sjss.getVar(names[colIndex]);
						}
						else {
							filledData = false;
							break;
						}
					}
					if(filledData) {
						list.add(count, values);
						count++;
					}
				}
			} 
			catch (RuntimeException e) {
				e.printStackTrace();
			}
			String nodeKey = nodeType;
			if(nodeKey.length()>31)nodeKey = nodeKey.substring(0, 31);
			if(!properties.isEmpty()) {
				hash.put(nodeKey, formatData(nodeType, properties, list));
			}
		}
		
		return hash;
	}

	/**
	 * Method formatData.  Formats the data into a vector of string arrays for processing.
	 * @param nodeType String
	 * @param properties HashSet<String>
	 * @param list ArrayList<Object[]>
	
	 * @return Vector<String[]> the results. */
	private Vector<String[]> formatData(String nodeType, HashSet<String> properties, ArrayList<Object[]> list) {
		Collections.sort(list, new Comparator<Object[]>() {
			public int compare(Object[] a, Object[] b) {
				return a[0].toString().compareTo(b[0].toString());
			}
		});
		String[] relation = {"Node", "Ignore"};
		list.add(0, relation);
		String[] header = new String[properties.size()+1];
		Iterator<String> it = properties.iterator();
		header[0] = nodeType;
		for(int i = 1; i <= properties.size(); i++) {
			header[i] = it.next();
		}
		list.add(1, header);
		Vector<String[]> results = new Vector<String[]>();
		for(Object[] o : list) {
			String[] toAdd = new String[o.length];
			for(int i = 0; i < o.length; i++) {
				toAdd[i] = o[i].toString();
			}
			results.add(toAdd);
		}
		
		return results;
	}
	
	/**
	 * Reorganize the data from querying the engine into a format similar to a Microsoft Excel Loading Sheet
	 * @param oldHash 		Hashtable containing the data from ExportNodeLoadSheetsListener, which gets the data from querying the engine						
	 * @return newHash		Hashtable<String,Vector<String[]>> containing the information in a format similar to the Microsoft Excel Loading Sheet
	 */
	private Hashtable<String, Vector<String[]>> prepareLoadingSheetExport(Hashtable oldHash) {
		Hashtable newHash = new Hashtable();
		Iterator<String> keyIt = oldHash.keySet().iterator();
		while(keyIt.hasNext()){
			String key = keyIt.next();
			Vector<String[]> sheetV = (Vector<String[]>) oldHash.get(key);
			Vector<String[]> newSheetV = new Vector<String[]>();
			String[] oldTopRow = sheetV.get(0);//this should be {Relation, *the relation, "", "" ...}
			String[] oldHeaderRow = sheetV.get(1);//this should be {*header1, *header2....}
			String[] oldSecondRow = new String[oldHeaderRow.length];//this is in case the sheet is null (other than the headers)
			if(sheetV.size()>2) oldSecondRow = sheetV.get(2);//this should be {*value1, *value2....}
			String[] newTopRow = new String[oldHeaderRow.length+1];
			String prev = "";

			newTopRow[0] = oldTopRow[0];
			for(int i = 0; i<oldHeaderRow.length;i++){
				newTopRow[i+1] = oldHeaderRow[i];
			}//newTopRow is now set as {"Relation", "Header1", "Header2", ...}
			newSheetV.add(newTopRow);

			ArrayList<String> headers = new ArrayList<String>();
			for(String s : newTopRow) {
				headers.add(s);
			}

			String[] newSecondRow = new String[oldHeaderRow.length+1];
			newSecondRow[0] = oldTopRow[1];
			for(int i = 0; i<oldSecondRow.length;i++){
				if(oldSecondRow[0] == null) { 
					continue;
				}
				int headerIndex = -1;
				if(oldSecondRow[1] != null) {
					headerIndex = headers.indexOf(oldSecondRow[1]);
				}
				if(headerIndex != -1) {
					newSecondRow[headerIndex] = oldSecondRow[2];
				}
				newSecondRow[1] = oldSecondRow[0];

				prev = oldSecondRow[0];
			}//newSecondRow should now be {*the relation, *value1....}
			newSheetV.add(newSecondRow);

			//now to run through the rest of the sheet
			for(int i = 3; i<sheetV.size(); i++) {		
				String[] row = sheetV.get(i);
				if(prev.equals(row[0])) {
					int headerIndex = -1;
					if(row[1] != null) {
						headerIndex = headers.indexOf(row[1]);
					}
					if(headerIndex != -1) {
						newSheetV.lastElement()[headerIndex] = row[2];	
					}
					continue;
				}

				String[] newRow = new String[headers.size()+1];
				int headerIndex = -1;
				if(row[1] != null) {
					headerIndex = headers.indexOf(row[1]);
				}
				if(headerIndex != -1) {
					newRow[headerIndex] = row[2];
				}
				newRow[1] = row[0];

				prev = row[0];
				newSheetV.add(newRow);
			}

			//now add the completed sheet to the new hash
			newHash.put(key, newSheetV);
		}
		return newHash;
	}
	
	/**
	 * Writes the sheet containing the information for all the node instances and properties for the node type specified by the user
	 * @param key 			String containing the name of the sheet to write
	 * @param sheetVector 	Vector<String[]> containing the data, all the node instance and properties, to export in the sheet
	 * @param workbook 		XSSFWorkbook to add the sheet to
	 */
	private void writeSheet(String key, Vector<String[]> sheetVector, XSSFWorkbook workbook) {
		XSSFSheet worksheet = workbook.createSheet(key);
		int count=0;//keeps track of rows; one below the row int because of header row
		final Pattern NUMERIC = Pattern.compile("^\\d+\\.?\\d*$");
		//for each row, create the row in excel
		for (int row=0; row<sheetVector.size();row++){
			XSSFRow row1 = worksheet.createRow( count);
			count++;
			//for each col, write it to that row.7
			for (int col=0; col<sheetVector.get(row).length;col++){
				XSSFCell cell = row1.createCell(col);
				if(sheetVector.get(row)[col] != null) {
					String val = sheetVector.get(row)[col];
					//Check if entire value is numeric - if so, set cell type and parseDouble, else write normally
					if(val != null && !val.isEmpty() && NUMERIC.matcher(val).find()) {
						cell.setCellType(CellType.NUMERIC);
						cell.setCellValue(Double.parseDouble(val));
					} else {
						cell.setCellValue(sheetVector.get(row)[col].replace("\"", ""));
					}
				}
			}
		}
	}

	public void setShowSuccessMessage(Boolean showSuccessMessage) {
		this.showSuccessMessage = showSuccessMessage;
	}
	
	public void setWriteFileName(String writeFileName) {
		this.writeFileName = writeFileName;
	}

}
