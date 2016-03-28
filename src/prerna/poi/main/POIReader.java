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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openrdf.sail.SailException;

import prerna.engine.api.IEngine;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

/**
 * Loading data into SEMOSS using Microsoft Excel Loading Sheet files
 */
public class POIReader extends AbstractFileReader {

	private static final Logger logger = LogManager.getLogger(POIReader.class.getName());

	private Hashtable <String, Hashtable <String, String>> concepts = new Hashtable <String, Hashtable <String, String>>();
	private Hashtable <String, Vector <String>> relations = new Hashtable <String, Vector<String>>();
	private Hashtable <String, String> sheets = new Hashtable <String, String> ();

	/**
	 * Load data into SEMOSS into an existing database
	 * 
	 * @param engineName
	 *            String grabbed from the user interface specifying which database to add the data
	 * @param fileNames
	 *            Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase
	 *            String grabbed from the user interface that is used as the URI base for all instances
	 * @param customMap
	 *            Absolute path that determines the location of the current db map file for the data
	 * @param owlFile
	 *            String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 * @throws EngineException
	 * @throws FileReaderException
	 * @throws FileWriterException
	 * @throws InvalidUploadFormatException
	 */
	//	public void importFileWithConnection(String engineName, String fileNames, String customBase, String customMap, String owlFile)
	public void importFileWithConnection(String engineName, String fileNames, String customBase, String owlFile)
			throws FileNotFoundException, IOException {
		logger.setLevel(Level.ERROR);
		String[] files = prepareReader(fileNames, customBase, owlFile, engineName);
		openEngineWithConnection(engineName);

		// load map file for existing db
		//		if (!customMap.isEmpty()) {
		//			openProp(customMap);
		//		}
		for (String fileName : files) {
			importFile(fileName);
		}
		loadMetadataIntoEngine();
		createBaseRelations();
		commitDB();
		engine.loadTransformedNodeNames();
	}

	/**
	 * Loading data into SEMOSS to create a new database
	 * 
	 * @param dbName
	 *            String grabbed from the user interface that would be used as the name for the database
	 * @param fileNames
	 *            Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase
	 *            String grabbed from the user interface that is used as the URI base for all instances
	 * @param customMap
	 *            Absolute path that determines the location of a custom map file for the data
	 * @param owlFile
	 *            String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 * @throws EngineException
	 * @throws FileReaderException
	 * @throws FileWriterException
	 * @throws InvalidUploadFormatException
	 */
	//	public void importFileWithOutConnection(String smssLocation, String engineName, String fileNames, String customBase, String customMap, String owlFile)
	public void importFileWithOutConnection(String smssLocation, String engineName, String fileNames, String customBase, String owlFile)
			throws FileNotFoundException, IOException {
		String[] files = prepareReader(fileNames, customBase, owlFile, smssLocation);
		try {
			openRdfEngineWithoutConnection(engineName);
			// load map file for db if user wants to use specific URIs
			//			if (!customMap.isEmpty()) {
			//				openProp(customMap);
			//			}
			// if user selected a map, load just as before--using the prop file to discover Excel->URI translation
			for (String fileName : files) {
				importFile(fileName);
			}
			loadMetadataIntoEngine();
			createBaseRelations();
		} finally {
			closeDB();
			closeOWL();
		}
	}

	public void importFileWithOutConnectionRDBMS(String smssLocation, String engineName, String fileNames, String customBase, String owlFile, SQLQueryUtil.DB_TYPE dbType, boolean allowDuplicates)
			throws FileNotFoundException, IOException {

		queryUtil = SQLQueryUtil.initialize(dbType);
		String[] files = prepareReader(fileNames, customBase, owlFile, smssLocation);
		try {
			openRdbmsEngineWithoutConnection(engineName);
			// if user selected a map, load just as before--using the prop file to discover Excel->URI translation
			for (String fileName : files) {
				importFileRDBMS(fileName);
			}
			commitDB();
			createBaseRelations();
			RDBMSEngineCreationHelper.writeDefaultQuestionSheet(engine);
		} finally {
			closeDB();
			closeOWL();
		}
	}

	/**
	 * Load subclassing information into the db and the owl file Requires the data to be in specific excel tab labeled "Subclass", with Parent nodes
	 * in the first column and child nodes in the second column
	 * 
	 * @param subclassSheet
	 *            Excel sheet with the subclassing information
	 * @throws IOException 
	 * @throws EngineException
	 * @throws SailException
	 */
	private void createSubClassing(XSSFSheet subclassSheet) throws IOException {
		// URI for subclass
		String pred = Constants.SUBCLASS_URI;
		// check parent and child nodes in correct position
		XSSFRow row = subclassSheet.getRow(0);
		String parentNode = row.getCell(0).toString().trim().toLowerCase();
		String childNode = row.getCell(1).toString().trim().toLowerCase();
		// check to make sure parent column is in the correct column
		if (!parentNode.equalsIgnoreCase("parent")) {
			throw new IOException("Error with Subclass Sheet.\nError in parent node column.");
			//			JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
			//			JOptionPane.showMessageDialog(playPane, "<html>Error with Subclass Sheet.<br>Error in parent node column.</html>");
		}
		// check to make sure child column is in the correct column
		if (!childNode.equalsIgnoreCase("child")) {
			throw new IOException("Error with Subclass Sheet.\nError in child node column.");
			//			JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
			//			JOptionPane.showMessageDialog(playPane, "<html>Error with Subclass Sheet.<br>Error in child node column.</html>");
		}
		// loop through and create all the triples for subclassing
		int lastRow = subclassSheet.getLastRowNum();
		for (int i = 1; i <= lastRow; i++) {
			row = subclassSheet.getRow(i);
			
			String parentURI = owler.addConcept( Utility.cleanString(row.getCell(0).toString(), true) );
			String childURI = owler.addConcept( Utility.cleanString(row.getCell(1).toString(), true) );
			// add triples to engine
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{childURI, pred, parentURI, true});
			// add triples to OWL
			owler.addSubclass(childNode, parentNode);
			//			baseEngCreator.addToBaseEngine(new Object[]{childNode, pred, parentNode, true});
			//			baseEngCreator.addToBaseEngine(new Object[]{childNode, pred, semossNodeURI, true});
			//			baseEngCreator.addToBaseEngine(new Object[]{parentNode, pred, semossNodeURI, true});
		}
		engine.commit();
		owler.commit();
	}

	/**
	 * Load the excel workbook, determine which sheets to load in workbook from the Loader tab
	 * @param fileName 					String containing the absolute path to the excel workbook to load
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void importFile(String fileName) throws FileNotFoundException, IOException {

		XSSFWorkbook workbook = null;
		FileInputStream poiReader = null;
		try {
			poiReader = new FileInputStream(fileName);
			workbook = new XSSFWorkbook(poiReader);
			// load the Loader tab to determine which sheets to load
			XSSFSheet lSheet = workbook.getSheet("Loader");
			if (lSheet == null) {
				throw new IOException("Could not find Loader Sheet in Excel file " + fileName);
			}
			// check if user is loading subclassing relationships
			XSSFSheet subclassSheet = workbook.getSheet("Subclass");
			if (subclassSheet != null) {
				createSubClassing(subclassSheet);
			}
			// determine number of sheets to load
			int lastRow = lSheet.getLastRowNum();
			// first sheet name in second row
			for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
				XSSFRow row = lSheet.getRow(rIndex);
				// check to make sure cell is not null
				if (row != null) {
					XSSFCell sheetNameCell = row.getCell(0);
					XSSFCell sheetTypeCell = row.getCell(1);
					if (sheetNameCell != null && sheetTypeCell != null) {
						// get the name of the sheet
						String sheetToLoad = sheetNameCell.getStringCellValue();
						// determine the type of load
						String loadTypeName = sheetTypeCell.getStringCellValue();
						if (!sheetToLoad.isEmpty() && !loadTypeName.isEmpty()) {
							logger.debug("Cell Content is " + sheetToLoad);
							// this is a relationship
							if (loadTypeName.contains("Matrix")) {
								loadMatrixSheet(sheetToLoad, workbook);
								engine.commit();
							} else {
								loadSheet(sheetToLoad, workbook);
								engine.commit();
							}
						}
					}
				}
			}

			//display names next
			XSSFSheet displayNamesSheet = workbook.getSheet("DisplayNames");
			if (displayNamesSheet != null) {
				lastRow = displayNamesSheet.getLastRowNum();
				for (int rIndex = 0; rIndex <= lastRow; rIndex++) {
					XSSFRow row = displayNamesSheet.getRow(rIndex);
					// check to make sure cell is not null
					if (row != null) {
						XSSFCell type = row.getCell(0);
						XSSFCell node = row.getCell(1);
						String displayName = null;
						XSSFCell property = null;
						String subject = "";
						if(type.getStringCellValue().equalsIgnoreCase("Node")){
							displayName = Utility.cleanString(row.getCell(2).toString(),true);
							subject = node.getStringCellValue();
						} else if(type.getStringCellValue().equalsIgnoreCase("Property")){
							property = row.getCell(2);
							displayName = Utility.cleanString(row.getCell(3).toString(),true);
							subject = node.getStringCellValue() + "%" + property.getStringCellValue();
						} else {
							continue;
						}
						if(!displayNamesHash.contains(displayName) && !displayNamesHash.containsKey(subject)){
							displayNamesHash.put(subject, displayName);
						}
					}
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new FileNotFoundException(e.getMessage());
			} else {
				throw new FileNotFoundException("Could not find Excel file located at " + fileName);
			}
		} catch (IOException e) {
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("Could not read Excel file located at " + fileName);
			}
		} catch (Exception e) {
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("File: " + fileName + " is not a valid Microsoft Excel (.xlsx, .xlsm) file");
			}
		} finally {
			if(poiReader != null) {
				try {
					poiReader.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new IOException("Could not close Excel file stream");
				}
			}
		}
	}


	private void assimilateSheet(String sheetName, XSSFWorkbook workbook)
	{
		// really simple job here
		// load the sheet
		// if you find it
		// get the first row
		// if the row is present 
		XSSFSheet lSheet = workbook.getSheet(sheetName);

		System.err.println("Processing Sheet..  " + sheetName);

		if(lSheet != null)
		{
			XSSFRow header = lSheet.getRow(0);
			// I will come to you shortly
			XSSFRow colPredictor = lSheet.getRow(1);
			int colLength = header.getLastCellNum();	
			int colcolLength = colPredictor.getLastCellNum();

			System.out.println(colLength + " <>" + colcolLength);

			if(header != null)
			{
				String type = header.getCell(0).getStringCellValue();
				if(type.equalsIgnoreCase("Relation"))
				{
					// process it as relation
					String fromName = header.getCell(1).getStringCellValue();
					fromName = Utility.cleanString(fromName, true);
					String toName = header.getCell(2).getStringCellValue();
					toName = Utility.cleanString(toName, true);

					Vector <String> relatedTo = new Vector<String>();
					if(relations.containsKey(fromName))
						relatedTo = relations.get(fromName);

					relatedTo.addElement(toName);
					relations.put(fromName, relatedTo);

					// if the concepts dont have relation key
					if(!concepts.containsKey(fromName))
					{
						Hashtable <String, String> props = new Hashtable<String, String>();
						props.put(fromName, "varchar(255)");
						concepts.put(fromName, props);
					}

					// if the concepts dont have relation key
					if(!concepts.containsKey(toName))
					{
						Hashtable <String, String> props = new Hashtable<String, String>();
						props.put(toName, "varchar(255)");
						concepts.put(toName, props);
					}

					sheets.put(fromName + "-" + toName, sheetName);
				}
				else
				{
					// now predict the columns
					String [] firstRowCells = getCells(colPredictor);
					String [] headers = getCells(header);
					String [] types = new String[headers.length];
					String [] initTypes = predictRowTypes(firstRowCells);

					int delta = types.length - initTypes.length;
					System.arraycopy(initTypes, 0, types, 0, initTypes.length);

					for(int deltaIndex = initTypes.length;deltaIndex < types.length;deltaIndex++)
						types[deltaIndex] = "varchar(800)";

					String conceptName = headers[1];

					conceptName = Utility.cleanString(conceptName, true);

					sheets.put(conceptName, sheetName);
					Hashtable <String, String> nodeProps= new Hashtable<String, String>();

					if(concepts.containsKey(conceptName))
						nodeProps = concepts.get(conceptName);

					/*else
					{
						nodeProps = new Hashtable<String, String>();
						nodeProps.put(conceptName, types[1]); // will change the varchar shortly
					}*/
					// process it as a concept
					for(int colIndex = 0;colIndex < types.length;colIndex++)
					{
						String thisName = headers[colIndex];
						if(thisName != null)
						{
							thisName = Utility.cleanString(thisName, true);
							nodeProps.put(thisName, types[colIndex]); // will change the varchar shortly
						}
					}

					concepts.put(conceptName, nodeProps);
				}
			}
		}
	}

	public String[] getCells(XSSFRow row)
	{
		int colLength = row.getLastCellNum();
		return getCells(row, colLength);
	}


	public String[] getCells(XSSFRow row, int totalCol)
	{
		int colLength = totalCol;
		String [] cols = new String[colLength];
		for(int colIndex = 1;colIndex < colLength;colIndex++)
		{
			XSSFCell thisCell = row.getCell(colIndex);
			// get all of this into a string
			if(thisCell != null && row.getCell(colIndex).getCellType() != Cell.CELL_TYPE_BLANK)
			{
				if(thisCell.getCellType() == Cell.CELL_TYPE_STRING)
				{
					cols[colIndex] = thisCell.getStringCellValue();
					cols[colIndex] = Utility.cleanString(cols[colIndex], true);
				}
				if(thisCell.getCellType() == Cell.CELL_TYPE_NUMERIC)
					cols[colIndex] = "" + thisCell.getNumericCellValue();
			}
			else
			{
				cols[colIndex] = "";
			}
		}	

		return cols;
	}

	public String[] predictRowTypes(String [] thisOutput)
	{
		String [] types = new String[thisOutput.length];
		String [] values = new String[thisOutput.length];

		for(int outIndex = 0;outIndex < thisOutput.length;outIndex++)
		{
			String curOutput = thisOutput[outIndex];
			//if(headers != null)
			//	System.out.println("Cur Output...  " + headers[outIndex] + " >> " + curOutput );
			Object [] cast = Utility.findTypes(curOutput);
			if(cast == null)
			{
				cast = new Object[2];
				cast[0] = "varchar(255)";
				cast[1] = ""; // make it into an empty String
			}
			types[outIndex] = cast[0] + "";
			values[outIndex] = cast[1] + "";

			//System.out.println(curOutput + types[outIndex] + " <<>>" + values[outIndex]);
		}    	
		return types;
	}


	/**
	 * Load the excel workbook, determine which sheets to load in workbook from the Loader tab
	 * @param fileName 					String containing the absolute path to the excel workbook to load
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void importFileRDBMS(String fileName) throws FileNotFoundException, IOException {

		XSSFWorkbook workbook = null;
		FileInputStream poiReader = null;
		try {
			poiReader = new FileInputStream(fileName);
			workbook = new XSSFWorkbook(poiReader);
			// load the Loader tab to determine which sheets to load
			XSSFSheet lSheet = workbook.getSheet("Loader");
			if (lSheet == null) {
				throw new IOException("Could not find Loader Sheet in Excel file " + fileName);
			}
			// check if user is loading subclassing relationships
			//			XSSFSheet subclassSheet = workbook.getSheet("Subclass");
			//			if (subclassSheet != null) {
			//				createSubClassing(subclassSheet);
			//			}
			// determine number of sheets to load
			int lastRow = lSheet.getLastRowNum();
			// first sheet name in second row
			// step one is to go through every sheet and determine what are the concepts within these sheets
			// along with their properties
			// would be kind of cool to guess their type as well

			for(int sIndex = 1;sIndex <= lastRow;sIndex++)
			{
				XSSFRow row = lSheet.getRow(sIndex);
				// check to make sure cell is not null
				if (row != null) {
					XSSFCell sheetNameCell = row.getCell(0);
					XSSFCell sheetTypeCell = row.getCell(1);				
					if (sheetNameCell != null && sheetTypeCell != null) {
						// get the name of the sheet
						String sheetToLoad = sheetNameCell.getStringCellValue();
						// determine the type of load
						String loadTypeName = sheetTypeCell.getStringCellValue();

						assimilateSheet(sheetToLoad, workbook);

						// assimilate this sheet and workbook
						// I need to have 2 hashes
						// hash 1 tells me
						// Concept - and all the given properties of this concept
						// hash 2 tells me
						// given this concept what are its relationships as a vector	
					}
				}
			}

			// the next thing is to look at relation ships and add the appropriate column that I want to the respective concepts

			System.out.println("Lucky !!" + concepts + " <> " + relations);
			System.out.println("Ok.. now what ?");
			synchronizeRelations();

			// now I need to create the tables
			Enumeration <String> conceptKeys = concepts.keys();
			while(conceptKeys.hasMoreElements())
			{
				String thisConcept = conceptKeys.nextElement();
				createTable(thisConcept);
				processTable(thisConcept, workbook);
			}
			// I need to first create all the concepts
			// then all the relationships
			Enumeration <String> relationConcepts = relations.keys();
			while(relationConcepts.hasMoreElements())
			{
				String thisConcept = relationConcepts.nextElement();
				Vector <String> allRels = relations.get(thisConcept);

				for(int toIndex = 0;toIndex < allRels.size();toIndex++)
					// now process each one of these things
					createRelations(thisConcept, allRels.elementAt(toIndex), workbook);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new FileNotFoundException(e.getMessage());
			} else {
				throw new FileNotFoundException("Could not find Excel file located at " + fileName);
			}
		} catch (IOException e) {
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("Could not read Excel file located at " + fileName);
			}
		} catch (Exception e) {
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("File: " + fileName + " is not a valid Microsoft Excel (.xlsx, .xlsm) file");
			}
		} finally {
			if(poiReader != null) {
				try {
					poiReader.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new IOException("Could not close Excel file stream");
				}
			}
		}
	}

	private void synchronizeRelations()
	{
		Enumeration <String> relationKeys = relations.keys();

		while(relationKeys.hasMoreElements())
		{
			String relKey = relationKeys.nextElement();
			Vector <String> theseRelations = relations.get(relKey);

			Hashtable <String, String> prop1 = concepts.get(relKey);
			//if(prop1 == null)
			//	prop1 = new Hashtable<String, String>();

			for(int relIndex = 0;relIndex < theseRelations.size();relIndex++)
			{
				String thisConcept = theseRelations.elementAt(relIndex);
				Hashtable <String, String> prop2 = concepts.get(thisConcept);
				//if(prop2 == null)
				//	prop2 = new Hashtable<String, String>();

				// affinity is used to which table to get when I get to this relation
				String affinity = relKey + "-" + thisConcept + "AFF"; // <-- right.. that is some random shit.. the kind of stuff that gets me in trouble
				// need to compare which one is bigger and if so add to the other one right now
				// I have no idea what is the logic here
				// I should be comparing the total number of records
				// oh well.. !!

				// I also need to record who has this piece
				// so when we get to the point of inserting I know what i am doing
				if(prop1.size() > prop2.size())
				{
					prop2.put(relKey + "_FK", prop1.get(relKey));
					concepts.put(thisConcept, prop2); // God am I ever sure of anything
					sheets.put(affinity, thisConcept);
				}
				else
				{
					prop1.put(thisConcept+"_FK", prop2.get(thisConcept));
					concepts.put(relKey, prop1); // God am I ever sure of anything
					sheets.put(affinity, relKey);
				}
			}
		}

	}

	private void createTable(String thisConcept)
	{
		Hashtable <String, String> props = concepts.get(thisConcept);

		String conceptType = props.get(thisConcept);

		// add it to OWL
		//owler.addConcept(thisConcept);

		String createString = "CREATE TABLE " + thisConcept + " (";
		createString = createString + " " + thisConcept + " " + conceptType;

		//owler.addProp(thisConcept, thisConcept, conceptType);

		props.remove(thisConcept);

		// while for create it is fine
		// I have to somehow figure out a way to get rid of most of the other stuff
		Enumeration <String> fields = props.keys();

		while(fields.hasMoreElements())
		{
			String fieldName = fields.nextElement();
			String fieldType = props.get(fieldName);
			createString = createString + " , " + fieldName + " " + fieldType; 

			// also add this to the OWLER
			if(!fieldName.equalsIgnoreCase(thisConcept) && !fieldName.endsWith("_FK"))
				owler.addProp(thisConcept, fieldName, fieldType);
		}

		props.put(thisConcept, conceptType);

		createString = createString + ")";
		System.out.println("Creator....  " + createString);
		try {
			engine.insertData(createString);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// now I say process this table ?

	}

	private void processTable(String conceptName, XSSFWorkbook workbook)
	{
		// this is where the stuff kicks in
		String sheetName = sheets.get(conceptName);
		if(sheetName != null)
		{
			XSSFSheet lSheet = workbook.getSheet(sheetName);
			XSSFRow thisRow = lSheet.getRow(0);

			String [] cells = getCells(thisRow);
			int totalCols = cells.length;
			String [] types = new String[cells.length];

			String inserter = "INSERT INTO " + conceptName + " ( ";

			for(int cellIndex = 1;cellIndex < cells.length;cellIndex++)
			{
				if(cellIndex == 1)
					inserter = inserter + cells[cellIndex];
				else
					inserter = inserter + " , " + cells[cellIndex];
				types[cellIndex] = concepts.get(conceptName).get(cells[cellIndex]);
			}
			inserter = inserter + ") VALUES ";
			int lastRow = lSheet.getLastRowNum();
			String values = "";
			for(int rowIndex = 1;rowIndex <= lastRow;rowIndex++)
			{
				thisRow = lSheet.getRow(rowIndex);
				String [] uCells = getCells(thisRow, totalCols);
				cells = Utility.castToTypes(uCells, types);
				values = "( " + cells[1];

				for(int cellIndex = 2;cellIndex < cells.length;cellIndex++)
					values = values + " , " + cells[cellIndex];

				values = values + ")";
				try {
					engine.insertData(inserter +  values);
					//					conn.createStatement().execute(inserter + values);
				} catch (Exception e) {
					System.out.println("Insert query...  " + inserter + values);
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(0);
				}

			}
		}			
	}

	private void createRelations(String fromName, String toName, XSSFWorkbook workbook)
	{
		// now come the relations
		// I need 
		String sheetName = sheets.get(fromName + "-" + toName);
		String tableToSet = sheets.get(fromName + "-" + toName + "AFF");


		System.out.println("Affinity is " + tableToSet);
		String tableToInsert = toName;
		if(tableToSet.equalsIgnoreCase(tableToInsert))
			tableToInsert = fromName;
		owler.addRelation(tableToInsert, tableToSet, null);
		// I have told folks not to do implicit reification so therefore I will ignore everything
		// i.e. NOOOOOO properties on relations

		// the aff is the table where I need to insert
		// which also means that is what I need to look up to insert

		// huge assumption here but
		String updateString = "";
		boolean update = false;
		if(concepts.get(tableToSet).size() <= 2)
		{
			//updateString = "MERGE INTO  " + tableToSet + "  KEY(" + tableToSet +") VALUES "; // + ", " + tableToInsert + "_FK) VALUES "; //+ " SET ";
			updateString = "INSERT INTO " + tableToSet + "(" + tableToSet + " ," +  tableToInsert + "_FK" + ") VALUES ";
			// this is the case for insert really
		}
		else
		{
			// this is an update
			updateString = "Update " + tableToSet + "  SET "; // + ", " + tableToInsert + "_FK) VALUES "; //+ " SET ";
			update = true;
		}


		XSSFSheet lSheet = workbook.getSheet(sheetName);
		int lastRow = lSheet.getLastRowNum();

		XSSFRow thisRow = lSheet.getRow(0);
		String [] headers = getCells(thisRow);
		// realistically it is only second and third
		headers[1] = Utility.cleanString(headers[1], true);
		headers[2] = Utility.cleanString(headers[2], true);

		int setter, inserter;
		if(headers[1].equalsIgnoreCase(tableToSet))
		{
			setter = 1;
			inserter = 2;
		}
		else
		{
			setter = 2;
			inserter = 1;
		}
		String values = "";
		for(int rowIndex = 1;rowIndex <= lastRow;rowIndex++)
		{
			thisRow = lSheet.getRow(rowIndex);
			String [] cells = getCells(thisRow);
			//String [] cells = Utility.castToTypes(uCells, types);
			values = "";
			//values = "" + tableToSet + "," +  tableToInsert + " VALUES ";
			if(!update)
				values = "( '" + cells[setter] + "' , '" + cells[inserter] + "')";
			else
				values = tableToSet + " = '" + cells[setter] + "' WHERE " + tableToInsert +  "_FK" + " = '" + cells[inserter] + "'";
			try {
				engine.insertData(updateString + values);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("update query...  " + updateString + values);
				System.exit(1);
			}
		}
		System.out.println("update query...  " + updateString + values);
	}

	/**
	 * Load specific sheet in workbook
	 * 
	 * @param sheetToLoad
	 *            String containing the name of the sheet to load
	 * @param workbook
	 *            XSSFWorkbook containing the sheet to load
	 * @throws IOException
	 */
	public void loadSheet(String sheetToLoad, XSSFWorkbook workbook) throws IOException {

		XSSFSheet lSheet = workbook.getSheet(sheetToLoad);
		if (lSheet == null) {
			throw new IOException("Could not find sheet " + sheetToLoad + " in workbook.");
		}
		logger.info("Loading Sheet: " + sheetToLoad);
		System.out.println(">>>>>>>>>>>>>>>>> " + sheetToLoad);
		int lastRow = lSheet.getLastRowNum() + 1;

		// Get the first row to get column names
		XSSFRow row = lSheet.getRow(0);

		// initialize variables
		String objectNode = "";
		String relName = "";
		Vector<String> propNames = new Vector<String>();

		// determine if relationship or property sheet
		String sheetType = row.getCell(0).getStringCellValue();
		String subjectNode = row.getCell(1).getStringCellValue();
		int currentColumn = 0;
		if (sheetType.equalsIgnoreCase("Relation")) {
			objectNode = row.getCell(2).getStringCellValue();
			// if relationship, properties start at column 2
			currentColumn++;
		}

		// determine property names for the relationship or node
		// colIndex starts at currentColumn+1 since if relationship, the object node name is in the second column
		int lastColumn = 0;
		for (int colIndex = currentColumn + 1; colIndex < row.getLastCellNum(); colIndex++) {
			// add property name to vector
			if (row.getCell(colIndex) != null) {
				propNames.addElement(row.getCell(colIndex).getStringCellValue());
				lastColumn = colIndex;
			}
		}
		logger.info("Number of Columns: " + (lastColumn + 1));

		// processing starts
		logger.info("Number of Rows: " + lastRow);
		for (int rowIndex = 1; rowIndex < lastRow; rowIndex++) {
			// first cell is the name of relationship
			XSSFRow nextRow = lSheet.getRow(rowIndex);

			if (nextRow == null) {
				continue;
			}

			// get the name of the relationship
			if (rowIndex == 1) {
				XSSFCell relCell = nextRow.getCell(0);
				if(relCell != null && !relCell.getStringCellValue().isEmpty()) {
					relName = nextRow.getCell(0).getStringCellValue();
				} else {
					if(sheetType.equalsIgnoreCase("Relation")) {
						throw new IOException("Need to define the relationship on sheet " + sheetToLoad);
					}
					relName = "Ignore";
				}
			}

			// set the name of the subject instance node to be a string
			if (nextRow.getCell(1) != null && nextRow.getCell(1).getCellType() != XSSFCell.CELL_TYPE_BLANK) {
				nextRow.getCell(1).setCellType(XSSFCell.CELL_TYPE_STRING);
			}

			// to prevent errors when java thinks there is a row of data when the row is empty
			XSSFCell instanceSubjectNodeCell = nextRow.getCell(1);
			String instanceSubjectNode = "";
			if (instanceSubjectNodeCell != null && instanceSubjectNodeCell.getCellType() != XSSFCell.CELL_TYPE_BLANK
					&& !instanceSubjectNodeCell.toString().isEmpty()) {
				instanceSubjectNode = nextRow.getCell(1).getStringCellValue();
			} else {
				continue;
			}

			// get the name of the object instance node if relationship
			String instanceObjectNode = "";
			int startCol = 1;
			int offset = 1;
			if (sheetType.equalsIgnoreCase("Relation")) {
				if(nextRow.getCell(2) != null) {
					nextRow.getCell(2).setCellType(XSSFCell.CELL_TYPE_STRING);
					XSSFCell instanceObjectNodeCell = nextRow.getCell(2);
					if (instanceObjectNodeCell != null && instanceObjectNodeCell.getCellType() != XSSFCell.CELL_TYPE_BLANK
							&& !instanceObjectNodeCell.toString().isEmpty()) {
						instanceObjectNode = nextRow.getCell(2).getStringCellValue();
					} else {
						continue;
					}
				}
				startCol++;
				offset++;
			}

			Hashtable<String, Object> propHash = new Hashtable<String, Object>();
			// process properties
			for (int colIndex = (startCol + 1); colIndex < nextRow.getLastCellNum(); colIndex++) {
				if (propNames.size() <= (colIndex - offset)) {
					continue;
				}
				String propName = propNames.elementAt(colIndex - offset).toString();
				String propValue = "";
				if (nextRow.getCell(colIndex) == null || nextRow.getCell(colIndex).getCellType() == XSSFCell.CELL_TYPE_BLANK
						|| nextRow.getCell(colIndex).toString().isEmpty()) {
					continue;
				}
				if (nextRow.getCell(colIndex).getCellType() == XSSFCell.CELL_TYPE_NUMERIC) {
					if (DateUtil.isCellDateFormatted(nextRow.getCell(colIndex))) {
						Date date = (Date) nextRow.getCell(colIndex).getDateCellValue();
						propHash.put(propName, date);
					} else {
						Double dbl = new Double(nextRow.getCell(colIndex).getNumericCellValue());
						propHash.put(propName, dbl);
					}
				} else {
					nextRow.getCell(colIndex).setCellType(XSSFCell.CELL_TYPE_STRING);
					propValue = nextRow.getCell(colIndex).getStringCellValue();
					propHash.put(propName, propValue);
				}
			}

			if (sheetType.equalsIgnoreCase("Relation")) {
				// adjust indexing since first row in java starts at 0
				logger.info("Processing Relationship Sheet: " + sheetToLoad + ", Row: " + (rowIndex + 1));
				createRelationship(subjectNode, objectNode, instanceSubjectNode, instanceObjectNode, relName, propHash);
			} else {
				addNodeProperties(subjectNode, instanceSubjectNode, propHash);
			}
			if (rowIndex == (lastRow - 1)) {
				logger.info("Done processing: " + sheetToLoad);
			}
		}
	}

	/**
	 * Load excel sheet in matrix format
	 * 
	 * @param sheetToLoad
	 *            String containing the name of the excel sheet to load
	 * @param workbook
	 *            XSSFWorkbook containing the name of the excel workbook
	 * @throws EngineException
	 */
	public void loadMatrixSheet(String sheetToLoad, XSSFWorkbook workbook) {
		XSSFSheet lSheet = workbook.getSheet(sheetToLoad);
		int lastRow = lSheet.getLastRowNum();
		logger.info("Number of Rows: " + lastRow);

		// Get the first row to get column names
		XSSFRow row = lSheet.getRow(0);
		// initialize variables
		String objectNodeType = "";
		String relName = "";
		boolean propExists = false;

		String sheetType = row.getCell(0).getStringCellValue();
		// Get the string that contains the subject node type, object node type, and properties
		String nodeMap = row.getCell(1).getStringCellValue();

		// check to see if properties exist
		String propertyName = "";
		StringTokenizer tokenProperties = new StringTokenizer(nodeMap, "@");
		String triple = tokenProperties.nextToken();
		if (tokenProperties.hasMoreTokens()) {
			propertyName = tokenProperties.nextToken();
			propExists = true;
		}

		StringTokenizer tokenTriple = new StringTokenizer(triple, "_");
		String subjectNodeType = tokenTriple.nextToken();
		if (sheetType.equalsIgnoreCase("Relation")) {
			relName = tokenTriple.nextToken();
			objectNodeType = tokenTriple.nextToken();
		}

		// determine object instance names for the relationship
		ArrayList<String> objectInstanceArray = new ArrayList<String>();
		int lastColumn = 0;
		for (int colIndex = 2; colIndex < row.getLastCellNum(); colIndex++) {
			objectInstanceArray.add(row.getCell(colIndex).getStringCellValue());
			lastColumn = colIndex;
		}
		// fix number of columns due to data shift in excel sheet
		lastColumn--;
		logger.info("Number of Columns: " + lastColumn);

		try {
			// process all rows (contains subject instances) in the matrix
			for (int rowIndex = 1; rowIndex <= lastRow; rowIndex++) {
				// boolean to determine if a mapping exists
				boolean mapExists = false;
				XSSFRow nextRow = lSheet.getRow(rowIndex);
				// get the name subject instance
				String instanceSubjectName = nextRow.getCell(1).getStringCellValue();
				// see what relationships are mapped between subject instances and object instances
				for (int colIndex = 2; colIndex <= lastColumn; colIndex++) {
					String instanceObjectName = objectInstanceArray.get(colIndex - 2);
					Hashtable<String, Object> propHash = new Hashtable<String, Object>();
					// store value in cell between instance subject and object in current iteration of loop
					XSSFCell matrixContent = nextRow.getCell(colIndex);
					// if any value in cell, there should be a mapping
					if (matrixContent != null) {
						if (propExists) {
							if (matrixContent.getCellType() == XSSFCell.CELL_TYPE_NUMERIC) {
								if (DateUtil.isCellDateFormatted(matrixContent)) {
									propHash.put(propertyName, (Date) matrixContent.getDateCellValue());
									mapExists = true;
								} else {
									propHash.put(propertyName, new Double(matrixContent.getNumericCellValue()));
									mapExists = true;
								}
							} else {
								// if not numeric, assume it is a string and check to make sure it is not empty
								if (!matrixContent.getStringCellValue().isEmpty()) {
									propHash.put(propertyName, matrixContent.getStringCellValue());
									mapExists = true;
								}
							}
						} else {
							mapExists = true;
						}
					}

					if (sheetType.equalsIgnoreCase("Relation") && mapExists) {
						logger.info("Processing" + sheetToLoad + " Row " + rowIndex + " Column " + colIndex);
						createRelationship(subjectNodeType, objectNodeType, instanceSubjectName, instanceObjectName, relName, propHash);
					} else {
						logger.info("Processing" + sheetToLoad + " Row " + rowIndex + " Column " + colIndex);
						addNodeProperties(subjectNodeType, instanceSubjectName, propHash);
					}
				}
				logger.info(instanceSubjectName);
			}
		} finally {
			logger.info("Done processing: " + sheetToLoad);
		}
	}

	public static void main(String [] args) throws FileNotFoundException, IOException
	{
		TestUtilityMethods.loadDIHelper();
		
		POIReader reader = new POIReader();
		// DATABASE WILL BE WRITTEN WHERE YOUR DB FOLDER IS IN A FOLDER WITH THE ENGINE NAME
		// SMSS file will not be created at the moment.. will add shortly
		String fileNames = "C:\\Users\\mahkhalil\\Desktop\\Medical_Devices_Data.xlsx";
		String smssLocation = "";
		String engineName = "test";
		String customBase = "http://semoss.org/ontologies";
		String owlFile = DIHelper.getInstance().getProperty("BaseFolder") + "\\db\\" + engineName + "\\" + engineName + "_OWL.OWL";
		SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;
		
		reader.importFileWithOutConnectionRDBMS(smssLocation, engineName, fileNames, customBase, owlFile, dbType, false);
	}

}
