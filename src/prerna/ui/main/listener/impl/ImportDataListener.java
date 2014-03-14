/*******************************************************************************
\ * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.io.FileInputStream;
import java.util.ArrayList;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import org.apache.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

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
	
	Logger logger = Logger.getLogger(getClass());
	
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
		processor.setBaseDirectory(System.getProperty("user.dir"));
		
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
		else if(typeSelection.equals("Natural Language Proessing"))
			importType = ImportDataProcessor.IMPORT_TYPE.NLP;
		
		String fileNames = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_FILE_FIELD)).getText();
		if(fileNames.equals(""))
		{
			Utility.showError("Please select a file to import");

		}
		String customBaseURI = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.BASE_URI_TEXT_FIELD)).getText();

		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		String repo = list.getSelectedValue()+"";
		
		boolean successfulImport = false;
		//if we are replacing data, need a more through check for the user
		//unless we are creating a new engine, though, we want to have the user confirm the import
		//need to separate it out because each processing requires different information
		if(importMethod == ImportDataProcessor.IMPORT_METHOD.OVERRIDE){
			boolean proceedWithImport = runOverrideCheck(fileNames);
			if(proceedWithImport){
				successfulImport = processor.processOverride(importType, customBaseURI, fileNames, repo);
			}
		}
		else if (importMethod == ImportDataProcessor.IMPORT_METHOD.ADD_TO_EXISTING){
			boolean proceedWithImport = runCheck();
			if(proceedWithImport)
				successfulImport = processor.processAddToExisting(importType, customBaseURI, fileNames, repo);
		}
		else if (importMethod == ImportDataProcessor.IMPORT_METHOD.CREATE_NEW){
			String mapFile = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.MAP_TEXT_FIELD)).getText();
			String dbPropFile = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.DB_PROP_TEXT_FIELD)).getText();
			String questionFile = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.QUESTION_TEXT_FIELD)).getText();
			String dbName = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.DB_NAME_FIELD)).getText();
			successfulImport = processor.processCreateNew(importType, customBaseURI, fileNames, dbName, mapFile, dbPropFile, questionFile);
		}
		else if(importMethod == ImportDataProcessor.IMPORT_METHOD.RDBMS) {
			String dbType = ((JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_DRIVER_COMBOBOX)).getSelectedItem().toString();
			String dbImportURL = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_URL_FIELD)).getText();
			String dbImportUsername = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_USERNAME_FIELD)).getText();
			char[] dbImportPW = ((JPasswordField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_PW_FIELD)).getPassword();
			String dbName = ((JTextField)DIHelper.getInstance().getLocalProp(Constants.DB_NAME_FIELD)).getText();
			successfulImport = processor.processNewRDBMS(customBaseURI, fileNames, dbName, dbType, dbImportURL, dbImportUsername, dbImportPW);
		}
		
		//finally, show whether or not successful
		if(successfulImport)
			Utility.showMessage("Your database has been successfully updated!");
		else
			Utility.showError("Import has failed.");
	}
	
	private boolean runOverrideCheck(String fileNames){

		String replacedString = "";
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		String[] files = fileNames.split(";");

		for(String file : files) {
			try{
				XSSFWorkbook book = new XSSFWorkbook(new FileInputStream(file));
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
			} catch (Exception e){
				logger.error(e);
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
