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
package prerna.poi.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Create a workbook containing data formated in the Microsoft Excel Sheet Format
 */
public class POIWriter {

//	/**
//	 * The main method is never called within SEMOSS
//	 * Used for testing purposes
//	 * @param args
//	 */
//	public static void main(String[] args) {
//		POIWriter writer = new POIWriter();
//		Hashtable<String, Vector<String[]>> blankHash = new Hashtable<String, Vector<String[]>>();
//		Vector<String[]> blankVect = new Vector<String[]>();
//		String[] blankStr = new String[2];
//		blankStr[0] = "Relation";
//		blankStr[1] = "bill2";
//		blankVect.add(blankStr);
//		String[] blankStr2 = new String[2];
//		blankStr2[0] = "2";
//		blankStr2[1] = "3";
//		blankVect.add(blankStr2);
//		String[] blankStr3 = new String[2];
//		blankStr3[0] = "4";
//		blankStr3[1] = "5";
//		blankVect.add(blankStr3);
//		blankHash.put("TEST SHEET", blankVect);
//		writer.runExport(blankHash, null, null, true);
//	}

	/**
	 * Writes the information passed through a hashtable to a workbook
	 * Reorganizes the information of the hashtable, if formatData is true, to be in the format of a loading sheet
	 * @param hash 			Hashtable containing the information
	 * @param writeFile 	String containing the path of the where to write the workbook
	 * @param readFile 		String containing the path to a file where the information in that file will be added to the created workbook
	 * @param formatData 	Boolean true when the information in the hashtable needs to be reorganized to look like a load sheet
	 */
	public void runExport(Hashtable<String, Vector<String[]>> hash, String writeFile, String readFile, boolean formatData){
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		if(writeFile == null || writeFile.isEmpty()) {
			writeFile = Constants.GLITEM_CORE_LOADING_SHEET;
		}
		if(readFile == null || readFile.isEmpty()) {
			readFile = "BaseGILoadingSheets.xlsx";
		}
		String folder = "export";
		String fileLoc = workingDir + System.getProperty("file.separator") + folder + System.getProperty("file.separator") + writeFile;
		String readFileLoc = workingDir + System.getProperty("file.separator") + folder + System.getProperty("file.separator") + readFile;

		ExportLoadingSheets(fileLoc, hash, readFileLoc, formatData);
	}

	/**
	 * Reorganizes the information from the hashtable, if formatData is true, into a format that is similar to a load sheet and saves it in a workbook
	 * The data is reorganized in a Hashtable<String, Vector<String[]>> where the keys become the sheet name and the instance data in the format Vector<String[]>  
	 * @param fileLoc 		String containing the path location to save the workbook
	 * @param hash 			Hashtable containing the information gotten from ExportRelationshipsLoadSheetsListener, which gets the data from querying the engine
	 * @param formatData 	Boolean true when the information in the hashtable needs to be reorganized to look like a load sheet
	 */
	public void ExportLoadingSheets(String fileLoc, Hashtable<String, Vector<String[]>> hash, String readFileLoc, boolean formatData){
		//create file
		XSSFWorkbook wb = getWorkbook(readFileLoc);
		if(wb == null) return;
		Hashtable<String, Vector<String[]>> preparedHash;
		if(formatData) {
			preparedHash = prepareLoadingSheetExport(hash);
		} else {
			preparedHash = hash;
		}
		XSSFSheet sheet = wb.createSheet("Loader");
		Vector<String[]> data = new Vector<String[]>();
		data.add(new String[]{"Sheet Name", "Type"});
		for(String key : preparedHash.keySet()) {
			data.add(new String[]{key, "Usual"});
		}
		int count=0;
		for (int row=0; row<data.size();row++){
			XSSFRow row1 = sheet.createRow(count);
			count++;

			for (int col=0; col<data.get(row).length;col++) {
				XSSFCell cell = row1.createCell(col);
				if(data.get(row)[col] != null) {
					cell.setCellValue(data.get(row)[col].replace("\"", ""));
				}
			}
		}

		Set<String> keySet = preparedHash.keySet();
		for(String key: keySet){
			Vector<String[]> sheetVector = preparedHash.get(key);
			writeSheet(key, sheetVector, wb);
		}

		Utility.writeWorkbook(wb, fileLoc);
	}

	/**
	 * Writes a relation sheet or a node property sheet
	 * @param key 			String containing the name of the sheet to write
	 * @param sheetVector 	Vector<String[]> containing the data, all the relationship instance and properties or all the node instance and properties, to export in the sheet
	 * @param workbook 		XSSFWorkbook to add the sheet to
	 */
	public void writeSheet(String key, Vector<String[]> sheetVector, XSSFWorkbook workbook){
		XSSFSheet worksheet = workbook.createSheet(key);
		int count=0;//keeps track of rows; one below the row int because of header row
		final Pattern NUMERIC = Pattern.compile("^\\d+.?\\d*$");
		//for each row, create the row in excel
		for (int row=0; row<sheetVector.size();row++){
			XSSFRow row1 = worksheet.createRow( count);
			count++;
			//for each col, write it to that row.
			for (int col=0; col<sheetVector.get(0).length;col++){
				XSSFCell cell = row1.createCell(col);
				String val = sheetVector.get(row)[col];
				if(val != null && !val.isEmpty() && NUMERIC.matcher(val).find()) {
					cell.setCellType(Cell.CELL_TYPE_NUMERIC);
					cell.setCellValue(Double.parseDouble(val));
					continue;
				}
				cell.setCellValue(sheetVector.get(row)[col]);
			}
		}
	}

	/**
	 * Loads an existing workbook and takes that information of that workbook and adds it to the workbook that is being created 
	 * @param readFileLoc 	String containing the path to the file to read
	 * @return wb			XSSFWorkbook that contains the information of the workbook that is read
	 */
	public XSSFWorkbook getWorkbook(String readFileLoc) {
		XSSFWorkbook wb = null;
		if(readFileLoc != null) {
			FileInputStream stream = null;
			try {
				File inFile = new File(readFileLoc);

				if(inFile.exists()){
					stream = new FileInputStream(inFile);
					wb = new XSSFWorkbook(stream);
					stream.close();
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try{
					if(stream!=null)
						stream.close();
				}catch(IOException e) {
					e.printStackTrace();
				}
			}
		} else {
			wb = new XSSFWorkbook();
		}
		return wb;
	}


	/**
	 * Reorganize the data from querying the engine into a format similar to a Microsoft Excel Loading Sheet
	 * @param oldHash 		Hashtable containing the data from ExportRelationshipsLoadSheetsListener, which gets the data from querying the engine						
	 * @return newHash		Hashtable<String,Vector<String[]>> containing the information in a format similar to the Microsoft Excel Loading Sheet
	 */
	public Hashtable<String, Vector<String[]>> prepareLoadingSheetExport(Hashtable<String, Vector<String[]>> oldHash){
		Hashtable<String, Vector<String[]>> newHash = new Hashtable<String, Vector<String[]>>();
		Iterator<String> keyIt = oldHash.keySet().iterator();
		while(keyIt.hasNext()){
			String key = keyIt.next();
			Vector<String[]> sheetV = (Vector<String[]>) oldHash.get(key);

			//This sheet is always empty, don't try to modify or add it to the new hash
			if(key.equals("Sys-DeployGLItem")) {
				continue;
			} //Relationships exports, already formatted correctly
			else if(key.equals("Sys-Data") || key.equals("Sys-BLU") || key.equals("Ser-Data") || key.equals("Ser-BLU") || key.equals("Sys-SysHWUpgradeGLItem")) {
				newHash.put(key, sheetV);
				continue;
			}

			Vector<String[]> newSheetV = new Vector<String[]>();
			String[] oldTopRow = sheetV.get(0);//this should be {Relation, *the relation, "", "" ...}
			String[] oldHeaderRow = sheetV.get(1);//this should be {*header1, *header2....}
			String[] oldSecondRow = new String[oldHeaderRow.length];//this is in case the sheet is null (other than the headers)
			if(sheetV.size()>2) oldSecondRow = sheetV.get(2);//this should be {*value1, *value2....}
			String[] newTopRow = new String[oldTopRow.length+1];

			newTopRow[0] = oldTopRow[0];
			for(int i = 0; i<oldTopRow.length;i++){
				newTopRow[i+1] = oldHeaderRow[i];
			}//newTopRow is now set as {"Relation", "Header1", "Header2", ...}
			newSheetV.add(newTopRow);

			String[] newSecondRow = new String[oldTopRow.length+1];
			newSecondRow[0] = oldTopRow[1];
			for(int i = 0; i<oldTopRow.length;i++){
				newSecondRow[i+1] = oldSecondRow[i];
			}//newSecondRow should now be {*the relation, *value1....}
			newSheetV.add(newSecondRow);

			//now to run through the rest of the sheet
			for(int i = 3; i<sheetV.size(); i++){
				String[] row = sheetV.get(i);
				String[] newRow = new String[row.length+1];
				for(int colIndx = 0; colIndx < row.length; colIndx++){
					newRow[colIndx+1] = row[colIndx];
				}
				newSheetV.add(newRow);
			}

			//now add the completed sheet to the new hash
			newHash.put(key, newSheetV);
		}
		return newHash;
	}

}
