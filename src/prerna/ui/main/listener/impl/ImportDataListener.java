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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.poi.main.helper.ImportOptions;
import prerna.ui.components.ImportDataProcessor;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Controls the loading of data into the tool from given files.
 * TODO split up processing out of the listener.
 */
public class ImportDataListener implements IChakraListener {

	JTextField view = null;
	
	static final Logger logger = LogManager.getLogger(ImportDataListener.class.getName());
	
	/**
	 * Method setModel.  Sets the model that the listener will access.
	 * @param model JComponent
	 */
	public void setModel(JComponent model) {
	
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		// get the import data
		// trigger the import
		JComboBox comboBox = (JComboBox) Utility.getDIHelperLocalProperty(Constants.IMPORT_COMBOBOX);
		JComboBox typeBox = (JComboBox) Utility.getDIHelperLocalProperty(Constants.IMPORT_TYPE_COMBOBOX);
		JCheckBox autoGenerateInsights = (JCheckBox) Utility.getDIHelperLocalProperty(Constants.AUTO_GENERATE_INSIGHTS_CHECK_BOX);
		
		ImportDataProcessor processor = new ImportDataProcessor();

		// create the options object
		ImportOptions options = new ImportOptions();
		options.setBaseFolder(Utility.getBaseFolder());
		
		// set the import method
		String selection = comboBox.getSelectedItem() + "";
		if(selection.equals("Create new database engine")) {
			options.setImportMethod(ImportOptions.IMPORT_METHOD.CREATE_NEW);
		} else if(selection.equals("Add to existing database engine")) {
			options.setImportMethod(ImportOptions.IMPORT_METHOD.ADD_TO_EXISTING);
		} 
//		else if(selection.equals("Modify/Replace data in existing engine")) {
//			options.setImportMethod(ImportOptions.IMPORT_METHOD.OVERRIDE);
//		}
		else if(selection.equals("Create new RDBMS connection")) {
			options.setImportMethod(ImportOptions.IMPORT_METHOD.CONNECT_TO_EXISTING_RDBMS);
		}
		
		// set the import type
		String typeSelection = typeBox.getSelectedItem() +"";
		if(typeSelection.equals("CSV format")) {
			options.setImportType(ImportOptions.IMPORT_TYPE.CSV);
		} else if(typeSelection.equals("Microsoft Excel Table Format")) {
			options.setImportType(ImportOptions.IMPORT_TYPE.EXCEL);
		} else if(typeSelection.equals("Microsoft Excel Loader Sheet Format")) {
			options.setImportType(ImportOptions.IMPORT_TYPE.EXCEL_POI);
		} else if(typeSelection.equals("Natural Language Processing")) {
			options.setImportType(ImportOptions.IMPORT_TYPE.NLP);
		} else if(typeSelection.equals("Optical Character Recognition + NLP")) {
			options.setImportType(ImportOptions.IMPORT_TYPE.OCR);
		}
		
		// current ui assumes everything is rdf
		options.setDbType(ImportOptions.DB_TYPE.RDF);
		
		// set the file names
		String fileNames = ((JTextField) Utility.getDIHelperLocalProperty(Constants.IMPORT_FILE_FIELD)).getText();
		if(fileNames.equals("")) {
			Utility.showError("Please select a file to import");
		}
		options.setFileLocation(fileNames);

		// set the custom base uri
		String customBaseURI = ((JTextField) Utility.getDIHelperLocalProperty(Constants.BASE_URI_TEXT_FIELD)).getText();
		options.setBaseUrl(customBaseURI);
		
		JList list = (JList) Utility.getDIHelperLocalProperty(Constants.REPO_LIST);
		String repoSelection = list.getSelectedValue()+"";
		String dbNameBox = ((JTextField) Utility.getDIHelperLocalProperty(Constants.DB_NAME_FIELD)).getText();

		//if we are replacing data, need a more through check for the user
		//unless we are creating a new engine, though, we want to have the user confirm the import
		//need to separate it out because each processing requires different information
		
		
		// engine name is either selected value on UI or what user entered in box
		String engineName = null;
		if(options.getImportMethod() == ImportOptions.IMPORT_METHOD.CREATE_NEW) {
			engineName = dbNameBox;
		} else {
			engineName = repoSelection;
		}
		options.setDbName(engineName);

		try {
//			if(options.getImportMethod() == ImportOptions.IMPORT_METHOD.OVERRIDE) {
//				// check with user first
//				boolean proceedWithImport = runOverrideCheck(fileNames);
//				if(proceedWithImport){
//					processor.runProcessor(options);
//				}
//			}
//			else {
				processor.runProcessor(options);
//			}
			
			Utility.showMessage("Your database has been successfully updated!");
		} catch(IOException ex) {
			logger.error(Constants.STACKTRACE, ex);
			Utility.showError("Import has failed.\n" + ex.getMessage());
			return;
		} catch(Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
			Utility.showError("Import has failed.\n" + ex.getMessage());
			return;
		}
		
		try {
			// need to wait until the database files are all written in order to run the auto generated insights
			// will get a null pointer for the engine if you do not wait
			Thread.sleep(1000);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
	}
	
	private boolean runOverrideCheck(String fileNames){

		String replacedString = "";
		JFrame playPane = (JFrame) Utility.getDIHelperLocalProperty(Constants.MAIN_FRAME);
		String[] files = fileNames.split(";");

		for(String file : files) {
			FileInputStream fileIn = null;
			XSSFWorkbook book = null;
			try{
				fileIn = new FileInputStream(file);
				book = new XSSFWorkbook(fileIn);
				XSSFSheet lSheet = book.getSheet("Loader");
				int lastRow = lSheet.getLastRowNum();
				
				ArrayList<String> nodes = new ArrayList<String>();
				ArrayList<String[]> relationships = new ArrayList<String[]>();
				for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
					XSSFRow sheetNameRow = lSheet.getRow(rIndex);
					XSSFCell cell = sheetNameRow.getCell(0);
					XSSFSheet sheet = book.getSheet(cell.getStringCellValue());
	
					XSSFRow row = sheet.getRow(0);
					String sheetType = "";
					if(row.getCell(0) != null) {
						sheetType = row.getCell(0).getStringCellValue();
					}
					if("Node".equalsIgnoreCase(sheetType)) {
						if(row.getCell(1) != null) {
							nodes.add(row.getCell(1).getStringCellValue());
						}
					}
					if("Relation".equalsIgnoreCase(sheetType)) {
						String subject = "";
						String object = "";
						String relationship = "";
						if(row.getCell(1) != null && row.getCell(2) != null) {
							subject = row.getCell(1).getStringCellValue();
							object = row.getCell(2).getStringCellValue();
	
							row = sheet.getRow(1);
							if(row.getCell(0) != null) {
								relationship = row.getCell(0).getStringCellValue();
							}
	
							relationships.add(new String[]{subject, relationship, object});
						}
					}
				}
				for(String node: nodes) replacedString = replacedString + node + "\n";
				for(String[] rel : relationships) replacedString = replacedString + rel[0] +" " + rel[1] + " " + rel[2]+"\n";
			} catch (RuntimeException e){
				logger.error(e);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				try{
					if(fileIn!=null)
						fileIn.close();
				}catch(IOException e) {
					e.printStackTrace();
				}
				if(book != null) {
			          try {
			        	  book.close();
			          } catch(IOException e) {
			            logger.error(Constants.STACKTRACE, e);
			          }
			        }
			}
		}
		Object[] buttons = {"Cancel", "Continue"};
		int response = JOptionPane.showOptionDialog(playPane, "This move cannot be undone.\nPlease make sure the excel file is formatted correctly and make a back up jnl file before continuing.\n\nThe following data will be replaced:\n\n" +
				replacedString +
				"\n" +
				"Would you still like to continue?", 
				"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
		return response == 1 ? true : false;
	}
	
	public boolean runCheck(){
		JFrame playPane = (JFrame) Utility.getDIHelperLocalProperty(Constants.MAIN_FRAME);
		Object[] buttons = {"Cancel Loading", "Continue With Loading"};
		int response = JOptionPane.showOptionDialog(playPane, "This move cannot be undone. Please make sure the excel file is formatted correctly \nand make a back up jnl file before continuing. Would you still like to continue?", 
				"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
		return response == 1 ? true : false;
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.view = (JTextField)view;
	}
	
	//TODO: this cleaning will not be necessary once insights are shifted to RDBMS
	private String cleanSpaces(String s) {
		while (s.contains("  ")){
			s = s.replace("  ", " ");
		}
		return s.replaceAll(" ", "_");
	}
}
