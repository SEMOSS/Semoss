/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.poi.main.OntologyFileWriter;
import prerna.poi.main.POIReader;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.specific.tap.GLItemGeneratorICDValidated;
import prerna.ui.components.specific.tap.GLItemGeneratorICDValidated.CHANGED_DB;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Produces an excel workbook in Microsoft Load Sheet format based of information in TAP_Core regarding GLItems for TAP_Cost
 * Adds the data produced into the TAP_Cost
 * Performed when btnUpdateCostDB is pressed
 */
public class CostDBUpdateListener extends AbstractListener {
	private String fileName = "";

	/**
	 * This is executed when the btnUpdateCostDB is pressed by the user
	 * Gets information from changedDBComboBox and costDBComboBox as to which db contains TAP_Cost information and which contains TAP_Core information
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		String filePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\";
		//Get changed and Cost DB params
		JComboBox<String> changedDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.CHANGED_DB_COMBOBOX);
		String changedDB = (String) changedDBComboBox.getSelectedItem();
		JComboBox<String> costDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.COST_DB_COMBOBOX);
		String costDB = (String) costDBComboBox.getSelectedItem();
		String owlFile = (String) DIHelper.getInstance().getProperty(costDB + "_" + Constants.OWL);
		String mapName = DIHelper.getInstance().getProperty(costDB + "_" + Constants.ONTOLOGY);
		JTextField customBaseURIField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.COST_DB_BASE_URI_FIELD);
		String customBaseURI = customBaseURIField.getText();
		
		GLItemGeneratorICDValidated glGen = new GLItemGeneratorICDValidated();
		
		if(changedDB.toLowerCase().contains("site")) {
			glGen.genList(CHANGED_DB.SITE);
			this.fileName = filePath + Constants.GLITEM_SITE_LOADING_SHEET;
		} else if(changedDB.toLowerCase().contains("core")) {
			glGen.genList(CHANGED_DB.CORE);
			this.fileName = filePath + Constants.GLITEM_CORE_LOADING_SHEET;
		}
		executeCostDBUpdate(costDB, this.fileName, customBaseURI, mapName, owlFile);
	}
	
	/**
	 * Runs the queries required to produce the excel workbook and then adds that workbook to the db selected by the user in costDBComboBox
	 * @param costDB 			String containing the name of the engine from costDBComboBox selected by the user, should contain the name of the TAP_Cost db
	 * @param file 				String containing the file path to the excel workbook created and to add to the TAP_Cost db
	 * @param customBaseURI 	String containing the instance level URI for the db inputed by the user in customBaseURIField
	 * @param mapName 			String containing the custom map file path for the selected engine from costDBComboBox
	 * @param owlFile 			String containing the owl file path for the selected engine from costDBComboBox
	 */
	public void executeCostDBUpdate(String costDB, String file, String customBaseURI, String mapName, String owlFile) {
		POIReader reader = new POIReader();
		FileInputStream fileIn = null;
		try {
			//Delete all nodes/relationships of specified types
			fileIn = new FileInputStream(file.replace(";", ""));
			XSSFWorkbook book = new XSSFWorkbook(fileIn);
			XSSFSheet lSheet = book.getSheet("Loader");
			int lastRow = lSheet.getLastRowNum();

			ArrayList<String> sheets = new ArrayList<String>();
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
			JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
			Object[] buttons = {"Cancel", "Continue"};
			String replacedString = "";
			int response = JOptionPane.showOptionDialog(playPane, "This move cannot be undone.\nPlease make sure the excel file is formatted correctly and make a back up Cost DB jnl file before continuing.\n\n" +
					"Would you still like to continue?\n", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
			
			JOptionPane.showMessageDialog(playPane, "The Cost DB Loading Sheet can be found here:\n\n" + file);
			
			if (response == 1)
			{
				String deleteQuery = "";
				UpdateProcessor proc = new UpdateProcessor();
				IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(costDB);
				proc.setEngine(engine);

				int numberNodes = nodes.size();
				if(numberNodes > 0) {
					for(String node : nodes) {
					deleteQuery = "DELETE {?s ?p ?prop. ?s ?x ?y} WHERE { {";
					deleteQuery += "SELECT ?s ?p ?prop ?x ?y WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
					deleteQuery += node;
					deleteQuery += "> ;} {?s ?x ?y} MINUS {?x <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation> ;} ";
					deleteQuery += "OPTIONAL{ {?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;} {?s ?p ?prop ;} } } } ";
					deleteQuery += "}";

					proc.setQuery(deleteQuery);
					proc.processQuery();
					}
				}

				int numberRelationships = relationships.size();
				if(numberRelationships > 0) {
					for(String[] rel : relationships) {
					deleteQuery = "DELETE {?in ?relationship ?out. ?relationship ?contains ?prop} WHERE { {";
					deleteQuery += "SELECT ?in ?relationship ?out ?contains ?prop WHERE { "+ 
							"{?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
					deleteQuery += rel[0];
					deleteQuery += "> ;}";

					deleteQuery += "{?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
					deleteQuery += rel[2];
					deleteQuery += "> ;}";

					deleteQuery += "{?relationship <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/";
					deleteQuery += rel[1];
					deleteQuery += "> ;} {?in ?relationship ?out ;} ";
					deleteQuery += "OPTIONAL { {?relationship ?contains ?prop ;} } } }";
					deleteQuery += "} ";

					proc.setQuery(deleteQuery);
					proc.processQuery();
					}
				}

				//run the reader
				reader.importFileWithConnection(costDB, file, customBaseURI, mapName, owlFile);

				//run the ontology augmentor

				OntologyFileWriter ontologyWriter = new OntologyFileWriter();
				ontologyWriter.runAugment(mapName, reader.conceptURIHash, reader.baseConceptURIHash, 
						reader.relationURIHash, reader.baseRelationURIHash,
						reader.basePropURI);

				Utility.showMessage("Your database has been successfully updated!");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			Utility.showError("Load has failed. Please make sure the loads sheets in the excel file are \nformatted correctly, and objects match the map file.");
		}finally{
			try{
				if(fileIn!=null)
					fileIn.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {
	}
}
