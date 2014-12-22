/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.error.FileWriterException;
import prerna.error.HeaderClassException;
import prerna.error.NLPException;
import prerna.ui.components.ImportDataProcessor;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
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
	public void setModel(JComponent model)
	{
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		// get the import data
		// trigger the import
		JComboBox comboBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.IMPORT_COMBOBOX);
		JComboBox typeBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.IMPORT_TYPE_COMBOBOX);
		
		ImportDataProcessor.IMPORT_METHOD importMethod = null;
		ImportDataProcessor.IMPORT_TYPE importType = null;
		ImportDataProcessor processor = new ImportDataProcessor();
		processor.setBaseDirectory(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER));
		
		String selection = comboBox.getSelectedItem() + "";
		if(selection.equals("Add to existing database engine"))
			importMethod = ImportDataProcessor.IMPORT_METHOD.ADD_TO_EXISTING;
		else if(selection.equals("Modify/Replace data in existing engine"))
			importMethod = ImportDataProcessor.IMPORT_METHOD.OVERRIDE;
		else if(selection.equals("Create new database engine"))
			importMethod = ImportDataProcessor.IMPORT_METHOD.CREATE_NEW;
		else if(selection.equals("Create new RDBMS connection"))
			importMethod = ImportDataProcessor.IMPORT_METHOD.RDBMS;
		
		String typeSelection = typeBox.getSelectedItem() +"";
		if(typeSelection.equals("CSV format"))
			importType = ImportDataProcessor.IMPORT_TYPE.CSV;
		else if(typeSelection.equals("Microsoft Excel format"))
			importType = ImportDataProcessor.IMPORT_TYPE.EXCEL;
		else if(typeSelection.equals("Natural Language Processing"))
			importType = ImportDataProcessor.IMPORT_TYPE.NLP;
		else if(typeSelection.equals("Optical Character Recognition + NLP"))
			importType = ImportDataProcessor.IMPORT_TYPE.OCR;
		
		String fileNames = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_FILE_FIELD)).getText();
		if(fileNames.equals(""))
		{
			Utility.showError("Please select a file to import");

		}
		String customBaseURI = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.BASE_URI_TEXT_FIELD)).getText();

		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		String repo = list.getSelectedValue()+"";
		
		//if we are replacing data, need a more through check for the user
		//unless we are creating a new engine, though, we want to have the user confirm the import
		//need to separate it out because each processing requires different information
		try {
			if(importMethod == ImportDataProcessor.IMPORT_METHOD.OVERRIDE){
				boolean proceedWithImport = runOverrideCheck(fileNames);
				if(proceedWithImport){
					processor.processOverride(importType, customBaseURI, fileNames, repo);
				}
			}
			else if (importMethod == ImportDataProcessor.IMPORT_METHOD.ADD_TO_EXISTING){
				boolean proceedWithImport = runCheck();
				if(proceedWithImport)
					processor.processAddToExisting(importType, customBaseURI, fileNames, repo);
			}
			else if (importMethod == ImportDataProcessor.IMPORT_METHOD.CREATE_NEW){
				String mapFile = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.MAP_TEXT_FIELD)).getText();
				String dbPropFile = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.DB_PROP_TEXT_FIELD)).getText();
				String questionFile = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.QUESTION_TEXT_FIELD)).getText();
				String dbName = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.DB_NAME_FIELD)).getText();
				processor.processCreateNew(importType, customBaseURI, fileNames, dbName, mapFile, dbPropFile, questionFile);
			}
			else if(importMethod == ImportDataProcessor.IMPORT_METHOD.RDBMS) {
				String dbType = ((JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_DRIVER_COMBOBOX)).getSelectedItem().toString();
				String dbImportURL = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_URL_FIELD)).getText();
				String dbImportUsername = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_USERNAME_FIELD)).getText();
				char[] dbImportPW = ((JPasswordField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_PW_FIELD)).getPassword();
				String dbName = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.DB_NAME_FIELD)).getText();
				processor.processNewRDBMS(customBaseURI, fileNames, dbName, dbType, dbImportURL, dbImportUsername, dbImportPW);
			}
			Utility.showMessage("Your database has been successfully updated!");
		} catch (EngineException ex) {
			ex.printStackTrace();
			Utility.showError("Import has failed.\n" + ex.getMessage());
		} catch (FileReaderException ex) {
			ex.printStackTrace();
			Utility.showError("Import has failed.\n" + ex.getMessage());
		} catch (HeaderClassException ex) {
			ex.printStackTrace();
			Utility.showError("Import has failed.\n" + ex.getMessage());
		} catch (FileWriterException ex) {
			ex.printStackTrace();
			Utility.showError("Import has failed.\n" + ex.getMessage());
		} catch (RuntimeException ex) {
			ex.printStackTrace();
			Utility.showError("Import has failed.\n");
		} catch (NLPException e1) {
			Utility.showError("Import has failed.\n");
		}
	}
	
	private boolean runOverrideCheck(String fileNames){

		String replacedString = "";
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		String[] files = fileNames.split(";");

		for(String file : files) {
			FileInputStream fileIn = null;
			try{
				fileIn = new FileInputStream(file);
				XSSFWorkbook book = new XSSFWorkbook(fileIn);
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
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
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
}
