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
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.openrdf.sail.SailException;

import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.poi.main.helper.ImportOptions;
import prerna.poi.main.helper.excel.ExcelParsing;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SQLQueryUtil;

/**
 * Loading data into SEMOSS using Microsoft Excel Loading Sheet files
 */
public class POIReader extends AbstractFileReader {

	private static final Logger logger = LogManager.getLogger(POIReader.class.getName());

	private Hashtable <String, Hashtable <String, String>> concepts = new Hashtable <String, Hashtable <String, String>>();
	private Hashtable <String, Vector <String>> relations = new Hashtable <String, Vector<String>>();
	private Hashtable <String, String> sheets = new Hashtable <String, String> ();

	private int indexUniqueId = 1;
	//	private List<String> recreateIndexList = new Vector<String>(); 
	private List<String> tempIndexAddedList = new Vector<String>();
	private List<String> tempIndexDropList = new Vector<String>();	

	/**
	 * Load data into SEMOSS into an existing database
	 * 
	 * @param engineId
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
	public void importFileWithConnection(ImportOptions options)
			throws FileNotFoundException, IOException {		

		String engineId = options.getEngineID();
		String fileNames = options.getFileLocations();
		String customBase = options.getBaseUrl();
		String owlFile = options.getOwlFileLocation();
		logger.setLevel(Level.ERROR);
		String[] files = prepareReader(fileNames, customBase, owlFile, engineId);
		openEngineWithConnection(engineId);

		for (String fileName : files) {
			importFile(fileName);
		}
		loadMetadataIntoEngine();
		createBaseRelations();
		RDFEngineCreationHelper.insertSelectConceptsAsInsights(engine, owler.getConceptualNodes());
		commitDB();
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
	public IEngine importFileWithOutConnection(ImportOptions options)
			throws FileNotFoundException, IOException {
		String smssLocation = options.getSMSSLocation();
		String engineName = options.getDbName();
		String fileNames = options.getFileLocations();
		String customBase = options.getBaseUrl();
		String owlFile = options.getOwlFileLocation();
		String appID = options.getEngineID();
		boolean error = false;
		logger.setLevel(Level.WARN);

		String[] files = prepareReader(fileNames, customBase, owlFile, smssLocation);
		try {
			openRdfEngineWithoutConnection(engineName, appID);
			for (String fileName : files) {
				importFile(fileName);
			}
			loadMetadataIntoEngine();
			createBaseRelations();
			RDFEngineCreationHelper.insertNewSelectConceptsAsInsights(engine, owler.getConceptualNodes());
		}  catch(FileNotFoundException e) {
			error = true;
			throw new FileNotFoundException(e.getMessage());
		} catch(IOException e) {
			error = true;
			throw new IOException(e.getMessage());
		} catch(Exception e) {
			error = true;
			throw new IOException(e.getMessage());
		} finally {
			if(error || autoLoad) {
				closeDB();
				closeOWL();
			} else {
				commitDB();
			}
		}

		return engine;
	}

	public IEngine importFileWithOutConnectionRDBMS(ImportOptions options) throws FileNotFoundException, IOException {
		String smssLocation = options.getSMSSLocation();
		String engineName = options.getDbName();
		String fileNames = options.getFileLocations();
		String customBase = options.getBaseUrl();
		String owlFile = options.getOwlFileLocation();
		RdbmsTypeEnum dbType = options.getRDBMSDriverType();
		boolean allowDuplicates = options.isAllowDuplicates();
		boolean error = false;
		queryUtil = SQLQueryUtil.initialize(dbType);
		String[] files = prepareReader(fileNames, customBase, owlFile, smssLocation);
		String appID = options.getEngineID();
		try {
			openRdbmsEngineWithoutConnection(engineName, appID);
			// if user selected a map, load just as before--using the prop file to discover Excel->URI translation
			for (String fileName : files) {
				importFileRDBMS(fileName);
			}
			commitDB();
			createBaseRelations();
			RDBMSEngineCreationHelper.insertAllTablesAsInsights(this.engine, this.owler);
		} catch(FileNotFoundException e) {
			error = true;
			throw new FileNotFoundException(e.getMessage());
		} catch(IOException e) {
			error = true;
			throw new IOException(e.getMessage());
		} catch(Exception e) {
			error = true;
			throw new IOException(e.getMessage());
		} finally {
			if(error || autoLoad) {
				closeDB();
				closeOWL();
			} else {
				commitDB();
			}
		}

		return engine;
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
	private void createSubClassing(Sheet subclassSheet) throws IOException {
		// URI for subclass
		String pred = Constants.SUBCLASS_URI;
		// check parent and child nodes in correct position
		Row row = subclassSheet.getRow(0);
		String parentNode = row.getCell(0).toString().trim().toLowerCase();
		String childNode = row.getCell(1).toString().trim().toLowerCase();
		// check to make sure parent column is in the correct column
		if (!parentNode.equalsIgnoreCase("parent")) {
			throw new IOException("Error with Subclass Sheet.\nError in parent node column.");
		}
		// check to make sure child column is in the correct column
		if (!childNode.equalsIgnoreCase("child")) {
			throw new IOException("Error with Subclass Sheet.\nError in child node column.");
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
		Workbook workbook = null;
		FileInputStream poiReader = null;
		try {
			poiReader = new FileInputStream(fileName);
			workbook = WorkbookFactory.create(poiReader);
			// load the Loader tab to determine which sheets to load
			Sheet lSheet = workbook.getSheet("Loader");
			if (lSheet == null) {
				throw new IOException("Could not find Loader Sheet in Excel file " + fileName);
			}
			// check if user is loading subclassing relationships
			Sheet subclassSheet = workbook.getSheet("Subclass");
			if (subclassSheet != null) {
				createSubClassing(subclassSheet);
			}
			// determine number of sheets to load
			int lastRow = lSheet.getLastRowNum();
			// first sheet name in second row
			for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
				Row row = lSheet.getRow(rIndex);
				// check to make sure cell is not null
				if (row != null) {
					Cell sheetNameCell = row.getCell(0);
					Cell sheetTypeCell = row.getCell(1);
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


	private void assimilateSheet(String sheetName, Workbook workbook)
	{
		// really simple job here
		// load the sheet
		// if you find it
		// get the first row
		// if the row is present 
		Sheet lSheet = workbook.getSheet(sheetName);

		System.err.println("Processing Sheet..  " + sheetName);

		// we need to convert from the generic data types from the FE to the sql specific types
		if(sqlHash.isEmpty()) {
			createSQLTypes();
		}

		if(lSheet != null)
		{
			Row header = lSheet.getRow(0);
			// I will come to you shortly
			Row colPredictor = lSheet.getRow(1);
			int colLength = header.getLastCellNum();	
			int colcolLength = colPredictor.getLastCellNum();

			System.out.println(colLength + " <>" + colcolLength);

			if(header != null)
			{
				String type = header.getCell(0).getStringCellValue();

				// we need to perform the correct type check regardless if it is a relationship or a node
				// we shouldn't be assuming a relationship can only exist for something of type string
				// as a note, this is however the case in rdf since relationships use URIs which are strings
				String [] initTypes = predictRowTypes(lSheet);

				// convert to sql types
				int numCols = initTypes.length;
				String[] sqlDataTypes = new String[numCols];
				for(int colIdx = 0; colIdx < numCols; colIdx++) {
					if(initTypes[colIdx] != null){	
						if(sqlHash.get(initTypes[colIdx]) == null)
							sqlDataTypes[colIdx] = initTypes[colIdx];
						else
							sqlDataTypes[colIdx] = sqlHash.get(initTypes[colIdx]);
					}
				}


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
						//props.put(fromName, initTypes[1]);
						props.put(fromName, sqlDataTypes[1]);
						concepts.put(fromName, props);
					}

					// if the concepts dont have relation key
					if(!concepts.containsKey(toName))
					{
						Hashtable <String, String> props = new Hashtable<String, String>();
						//props.put(toName, initTypes[2]);
						props.put(toName, sqlDataTypes[2]);
						concepts.put(toName, props);
					}

					sheets.put(fromName + "-" + toName, sheetName);
				}
				else
				{
					// now predict the columns
					//					String [] firstRowCells = getCells(colPredictor);
					String [] headers = getCells(header);
					String [] types = new String[headers.length];

					//					int delta = types.length - initTypes.length;
					//System.arraycopy(initTypes, 0, types, 0, initTypes.length);
					System.arraycopy(sqlDataTypes, 0, types, 0, sqlDataTypes.length);

					//for(int deltaIndex = initTypes.length;deltaIndex < types.length;deltaIndex++)
					for(int deltaIndex = sqlDataTypes.length;deltaIndex < types.length;deltaIndex++)
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

	public String[] getCells(Row row)
	{
		int colLength = row.getLastCellNum();
		return getCells(row, colLength);
	}

	public String[] getCells(Row row, int totalCol)
	{
		int colLength = totalCol;
		String [] cols = new String[colLength];
		for(int colIndex = 1;colIndex < colLength;colIndex++)
		{
			Cell thisCell = row.getCell(colIndex);
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

	public String getCell(Cell thisCell) {
		if(thisCell != null && thisCell.getCellType() != Cell.CELL_TYPE_BLANK)
		{
			if(thisCell.getCellType() == Cell.CELL_TYPE_STRING) {
				return thisCell.getStringCellValue();
			}
			else if(thisCell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
				return thisCell.getNumericCellValue() + "";
			}
		}
		return "";
	}

	public String[] predictRowTypes(Sheet lSheet)
	{
		int numRows = lSheet.getLastRowNum();

		Row header = lSheet.getRow(0);
		int numCells = header.getLastCellNum();

		String [] types = new String[numCells];

		// need to loop through and make sure types are good
		// we know the first col is always null as it is not used
		for(int i = 1; i < numCells; i++) {
			String type = null;
			ROW_LOOP : for(int j = 1; j < numRows; j++) {
				Row row = lSheet.getRow(j);
				if(row != null) {
					Cell cell = row.getCell(i);
					if(cell != null) {
						String val = getCell(cell);
						if(val.isEmpty()) {
							continue ROW_LOOP;
						}
						String newTypePred = (Utility.findTypes(val)[0] + "").toUpperCase();
						if(newTypePred.contains("VARCHAR")) {
							type = newTypePred;
							break ROW_LOOP;
						}

						// need to also add the type null check for the first row
						if(!newTypePred.equals(type) && type != null) {
							// this means there are multiple types in one column
							// assume it is a string 
							if( (type.equals("BOOLEAN") || type.equals("INT") || type.equals("DOUBLE")) && 
									(newTypePred.equals("INT") || newTypePred.equals("INT") || newTypePred.equals("DOUBLE") ) ){
								// for simplicity, make it a double and call it a day
								// TODO: see if we want to impl the logic to choose the greater of the newest
								// this would require more checks though
								type = "DOUBLE";
							} else {
								// should only enter here when there are numbers and dates
								// TODO: need to figure out what to handle this case
								// for now, making assumption to put it as a string
								type = "VARCHAR(800)";
								break ROW_LOOP;
							}
						} else {
							// type is the same as the new predicated type
							// or type is null on first iteration
							type = newTypePred;
						}
					}
				}
			}
			if(type == null) {
				// no data for column....
				types[i] = "varchar(255)";
			} else {
				types[i] = type;
			}
		}

		return types;
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
		Workbook workbook = null;
		FileInputStream poiReader = null;
		try {
			poiReader = new FileInputStream(fileName);
			workbook = WorkbookFactory.create(poiReader);
			// load the Loader tab to determine which sheets to load
			Sheet lSheet = workbook.getSheet("Loader");
			if (lSheet == null) {
				throw new IOException("Could not find Loader Sheet in Excel file " + fileName);
			}
			int lastRow = lSheet.getLastRowNum();
			// first sheet name in second row
			// step one is to go through every sheet and determine what are the concepts within these sheets
			// along with their properties
			// would be kind of cool to guess their type as well

			for(int sIndex = 1;sIndex <= lastRow;sIndex++)
			{
				Row row = lSheet.getRow(sIndex);
				// check to make sure cell is not null
				if (row != null) {
					int sheetCounter = 0;
					Cell sheetNameCell = row.getCell(0);
					// unlike rdf, we do not have multiple load types for sheets
					//					XSSFCell sheetTypeCell = row.getCell(1);				
					if (sheetNameCell != null) {
						sheetCounter++;
						// get the name of the sheet
						String sheetToLoad = sheetNameCell.getStringCellValue();
						// determine the type of load
						//						String loadTypeName = sheetTypeCell.getStringCellValue();

						assimilateSheet(sheetToLoad, workbook);

						// assimilate this sheet and workbook
						// I need to have 2 hashes
						// hash 1 tells me
						// Concept - and all the given properties of this concept
						// hash 2 tells me
						// given this concept what are its relationships as a vector	
					}
					if(sheetCounter == 0) {
						throw new IOException("Loader sheet specified no sheets to upload.\n Please specify which sheets you want to laod.");
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
				if(!allRels.isEmpty()) {
					createRelations(thisConcept, allRels, workbook);
				}

				//				for(int toIndex = 0;toIndex < allRels.size();toIndex++)
				//					// now process each one of these things
				//					createRelations(thisConcept, allRels.elementAt(toIndex), workbook);
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
				//				if(prop1.size() > prop2.size())
				//				{
				//					prop2.put(relKey + "_FK", prop1.get(relKey));
				//					concepts.put(thisConcept, prop2); // God am I ever sure of anything
				//					sheets.put(affinity, thisConcept);
				//				}
				//				else
				//				{

				// due to loops, can't use the previous logic to determine FK based on who has less props
				// TODO: need to enable using the * to determine the FK position like in CSV files
				prop1.put(thisConcept+"_FK", prop2.get(thisConcept));
				concepts.put(relKey, prop1); // God am I ever sure of anything
				sheets.put(affinity, relKey);
				//				}
			}
		}

	}

	private void createTable(String thisConcept)
	{
		Hashtable <String, String> props = concepts.get(thisConcept);

		String conceptType = props.get(thisConcept);

		// add it to OWL
		owler.addConcept(thisConcept, conceptType);

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

	private void processTable(String conceptName, Workbook workbook)
	{
		// this is where the stuff kicks in
		String sheetName = sheets.get(conceptName);
		if(sheetName != null)
		{
			Sheet lSheet = workbook.getSheet(sheetName);
			Row thisRow = lSheet.getRow(0);

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
				if(types[1].equals("INT") || types[1].equals("DOUBLE")) {
					values = "( " + cells[1];
				} else {
					values = "( '" + cells[1] + "'";
				}
				for(int cellIndex = 2;cellIndex < cells.length;cellIndex++) {
					if(types[cellIndex].equals("INT") || types[cellIndex].equals("DOUBLE")) {
						values = values + " , " + cells[cellIndex];
					} else {
						values = values + " , '" + cells[cellIndex] + "'";
					}
				}

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

	private void createRelations(String fromName, List<String> toNameList, Workbook workbook)
	{
		int size = toNameList.size();
		List<String> relsAdded = new ArrayList<String>();

		for(int i = 0; i < size; i++) {
			String toName = toNameList.get(i);

			String sheetName = sheets.get(fromName + "-" + toName);
			Sheet lSheet = workbook.getSheet(sheetName);

			int lastRow = lSheet.getLastRowNum();
			Row thisRow = lSheet.getRow(0);
			String [] headers = getCells(thisRow);
			// realistically it is only second and third
			headers[1] = Utility.cleanString(headers[1], true);
			headers[2] = Utility.cleanString(headers[2], true);

			String tableToSet = headers[1];
			String tableToInsert = headers[2];
			//			String tableToSet = sheets.get(fromName + "-" + toName + "AFF");
			//
			//			System.out.println("Affinity is " + tableToSet);
			//			String tableToInsert = toName;
			//
			//			// TODO: what is this if statement for?
			//			if(tableToSet.equalsIgnoreCase(tableToInsert)) {
			//				tableToInsert = fromName;
			//			}

			// we need to make sure we create the predicate appropriately
			String predicate = null;
			int setter, inserter;
			if(headers[1].equalsIgnoreCase(tableToSet))
			{
				// this means the FK is on the tableToSet
				setter = 1;
				inserter = 2;
				predicate = tableToSet + "." + tableToInsert + "_FK." + tableToInsert + "." + tableToInsert;
			}
			else
			{
				// this means the FK is on the tableToInsert
				setter = 2;
				inserter = 1;
				predicate = tableToSet + "." + tableToSet + "." + tableToInsert + "." + tableToSet + "_FK";
			}
			owler.addRelation(tableToSet, tableToInsert, predicate);


			createIndices(tableToSet, tableToSet);

			for(int rowIndex = 1;rowIndex <= lastRow;rowIndex++)
			{
				thisRow = lSheet.getRow(rowIndex);
				String [] cells = getCells(thisRow);

				if(cells[setter] == null || cells[setter].isEmpty() || cells[inserter] == null || cells[inserter].isEmpty()) {
					continue; // why is there an empty in the excel sheet....
				}

				// need to determine if i am performing an update or an insert
				String getRowCountQuery = "SELECT COUNT(*) as ROW_COUNT FROM " + tableToSet + " WHERE " + 
						tableToSet + " = '" + cells[setter] + "' AND " + tableToInsert +  "_FK IS NULL";
				boolean isInsert = false;
				IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, getRowCountQuery);
				if(wrapper.hasNext()){
					String rowcount = wrapper.next().getValues()[0].toString();
					if(rowcount.equals("0")){
						isInsert = true;
					}
				}

				if(isInsert) {
					// we want to pull all concept values from query
					String colsToSelect = "";
					List<String> cols = new ArrayList<String>();
					Hashtable<String, String> propsToSelect = concepts.get(tableToSet);
					for(String prop : propsToSelect.keySet()) {
						if(prop.equalsIgnoreCase(tableToSet) || prop.endsWith("_FK")) {
							continue;
						}

						cols.add(prop);
						if(colsToSelect.isEmpty()) {
							colsToSelect = prop;
						} else {
							colsToSelect = colsToSelect + ", " + prop;
						}
					}
					// only need to be concerned with the relations that have been added
					for(String rel : relsAdded) {
						cols.add(rel);
						colsToSelect = colsToSelect + ", " + rel;
					}
					// will always have rel and col
					cols.add(tableToSet);
					cols.add(tableToInsert +  "_FK");
					colsToSelect = colsToSelect + ", " + tableToSet + ", " + tableToInsert +  "_FK";

					// is it a straight insert since there are only two columns
					if(cols.size() == 2) {
						String insert = "INSERT INTO " + tableToSet + "(" + tableToSet + " ," +  tableToInsert + "_FK" + 
								") VALUES ( '" + cells[setter] + "' , '" + cells[inserter] + "')";
						try {
							engine.insertData(insert);
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						// need to generate query to pull all existing information
						// then append the new relationship
						// and insert all those cells

						List<String> unknownColsList = new ArrayList<String>();
						String unknownCols = "";
						int numCols = cols.size();
						for(int colNum = 0; colNum < numCols; colNum++) {
							String col = cols.get(colNum);
							if(col.equalsIgnoreCase(tableToSet) || col.equalsIgnoreCase(tableToInsert +  "_FK")) {
								continue;
							}

							// plus 3 since last two in col should go into the if statement above
							if(colNum + 3 == numCols) {
								unknownCols += col + " ";
								unknownColsList.add(col);
							} else {
								unknownColsList.add(col);
								unknownCols += col + ", ";
							}
						}

						String existingValues = "(SELECT DISTINCT " + unknownCols + " FROM " + tableToSet + 
								" WHERE " + tableToSet + "='" + cells[setter] + "' ) AS TEMP_FK";
						StringBuilder selectingValues = new StringBuilder();
						selectingValues.append("SELECT DISTINCT ");

						for(int colNum = 0; colNum < numCols; colNum++) {
							String col = cols.get(colNum);
							if(unknownColsList.contains(col)) {
								selectingValues.append("TEMP_FK.").append(col).append(" AS ").append(col).append(", ");
							}
						}
						selectingValues.append("'" + cells[setter] + "'").append(" AS ").append(tableToSet).append(", ");
						selectingValues.append("'" + cells[inserter] + "'").append(" AS ").append(tableToInsert + "_FK").append(" ");
						selectingValues.append(" FROM ").append(tableToSet).append(",");

						String insert = "INSERT INTO " + tableToSet + "(" + colsToSelect + " ) " + selectingValues.toString() + existingValues;
						try {
							engine.insertData(insert);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				} else {
					// this is a nice and simple insert
					String updateString = "Update " + tableToSet + "  SET ";
					String values = tableToInsert +  "_FK" + " = '" + cells[inserter] + "' WHERE " + tableToSet + " = '" + cells[setter] + "'";
					try {
						engine.insertData(updateString + values);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			relsAdded.add(tableToInsert +  "_FK");
		}
	}


	private void createIndices(String cleanTableKey, String indexStr) {
		String indexOnTable =  cleanTableKey + " ( " +  indexStr + " ) ";
		String indexName = "INDX_" + cleanTableKey + indexUniqueId;
		String createIndex = "CREATE INDEX " + indexName + " ON " + indexOnTable;
		String dropIndex = queryUtil.getDialectDropIndex(indexName, cleanTableKey);//"DROP INDEX " + indexName;
		if(tempIndexAddedList.size() == 0 ){
			try {
				engine.insertData(createIndex);
			} catch (Exception e) {
				e.printStackTrace();
			}
			tempIndexAddedList.add(indexOnTable);
			tempIndexDropList.add(dropIndex);
			indexUniqueId++;
		} else {
			boolean indexAlreadyExists = false; 
			for(String index : tempIndexAddedList){
				if(index.equals(indexOnTable)){//TODO check various order of keys since they are comma seperated
					indexAlreadyExists = true;
					break;
				}

			}
			if(!indexAlreadyExists){
				try {
					engine.insertData(createIndex);
				} catch (Exception e) {
					e.printStackTrace();
				}
				tempIndexDropList.add(dropIndex);
				tempIndexAddedList.add(indexOnTable);
				indexUniqueId++;
			}
		}
	}


	//	private void createRelations(String fromName, String toName, XSSFWorkbook workbook)
	//	{
	//		// now come the relations
	//		// I need 
	//		String sheetName = sheets.get(fromName + "-" + toName);
	//		String tableToSet = sheets.get(fromName + "-" + toName + "AFF");
	//
	//
	//		System.out.println("Affinity is " + tableToSet);
	//		String tableToInsert = toName;
	//		if(tableToSet.equalsIgnoreCase(tableToInsert))
	//			tableToInsert = fromName;
	//		owler.addRelation(tableToInsert, tableToSet, null);
	//		// I have told folks not to do implicit reification so therefore I will ignore everything
	//		// i.e. NOOOOOO properties on relations
	//
	//		// the aff is the table where I need to insert
	//		// which also means that is what I need to look up to insert
	//
	//		// huge assumption here but
	//		String updateString = "";
	//		boolean update = false;
	//		if(concepts.get(tableToSet).size() <= 2)
	//		{
	//			//updateString = "MERGE INTO  " + tableToSet + "  KEY(" + tableToSet +") VALUES "; // + ", " + tableToInsert + "_FK) VALUES "; //+ " SET ";
	//			updateString = "INSERT INTO " + tableToSet + "(" + tableToSet + " ," +  tableToInsert + "_FK" + ") VALUES ";
	//			// this is the case for insert really
	//		}
	//		else
	//		{
	//			// this is an update
	//			updateString = "Update " + tableToSet + "  SET "; // + ", " + tableToInsert + "_FK) VALUES "; //+ " SET ";
	//			update = true;
	//		}
	//
	//
	//		XSSFSheet lSheet = workbook.getSheet(sheetName);
	//		int lastRow = lSheet.getLastRowNum();
	//
	//		XSSFRow thisRow = lSheet.getRow(0);
	//		String [] headers = getCells(thisRow);
	//		// realistically it is only second and third
	//		headers[1] = Utility.cleanString(headers[1], true);
	//		headers[2] = Utility.cleanString(headers[2], true);
	//
	//		int setter, inserter;
	//		if(headers[1].equalsIgnoreCase(tableToSet))
	//		{
	//			setter = 1;
	//			inserter = 2;
	//		}
	//		else
	//		{
	//			setter = 2;
	//			inserter = 1;
	//		}
	//		String values = "";
	//		for(int rowIndex = 1;rowIndex <= lastRow;rowIndex++)
	//		{
	//			thisRow = lSheet.getRow(rowIndex);
	//			String [] cells = getCells(thisRow);
	//			//String [] cells = Utility.castToTypes(uCells, types);
	//			values = "";
	//			//values = "" + tableToSet + "," +  tableToInsert + " VALUES ";
	//			if(!update) {
	//				values = "( '" + cells[setter] + "' , '" + cells[inserter] + "')";
	//			} else {
	//				values = tableToSet + " = '" + cells[setter] + "' WHERE " + tableToInsert +  "_FK" + " = '" + cells[inserter] + "'";
	//			}
	//			try {
	//				engine.insertData(updateString + values);
	//			} catch (Exception e) {
	//				// TODO Auto-generated catch block
	//				e.printStackTrace();
	//				System.out.println("update query...  " + updateString + values);
	//				System.exit(1);
	//			}
	//		}
	//		System.out.println("update query...  " + updateString + values);
	//	}

	/**
	 * Load specific sheet in workbook
	 * 
	 * @param sheetToLoad
	 *            String containing the name of the sheet to load
	 * @param workbook
	 *            XSSFWorkbook containing the sheet to load
	 * @throws IOException
	 */
	public void loadSheet(String sheetToLoad, Workbook workbook) throws IOException {
		Sheet lSheet = workbook.getSheet(sheetToLoad);
		if (lSheet == null) {
			throw new IOException("Could not find sheet " + sheetToLoad + " in workbook.");
		}
		logger.info("Loading Sheet: " + sheetToLoad);
		System.out.println(">>>>>>>>>>>>>>>>> " + sheetToLoad);
		int lastRow = lSheet.getLastRowNum() + 1;

		// Get the first row to get column names
		Row row = lSheet.getRow(0);

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
			Row nextRow = lSheet.getRow(rowIndex);

			if (nextRow == null) {
				continue;
			}

			// get the name of the relationship
			if (rowIndex == 1) {
				Cell relCell = nextRow.getCell(0);
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
			if (nextRow.getCell(1) != null && nextRow.getCell(1).getCellType() != Cell.CELL_TYPE_BLANK) {
				nextRow.getCell(1).setCellType(Cell.CELL_TYPE_STRING);
			}

			// to prevent errors when java thinks there is a row of data when the row is empty
			Cell instanceSubjectNodeCell = nextRow.getCell(1);
			String instanceSubjectNode = "";
			if (instanceSubjectNodeCell != null && instanceSubjectNodeCell.getCellType() != Cell.CELL_TYPE_BLANK
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
					// make it a string so i can easily parse it
					nextRow.getCell(2).setCellType(Cell.CELL_TYPE_STRING);
					Cell instanceObjectNodeCell = nextRow.getCell(2);
					// if empty, ignore
					if(ExcelParsing.isEmptyCell(instanceObjectNodeCell)) {
						continue;
					}
					instanceObjectNode = nextRow.getCell(2).getStringCellValue();
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
				// ignore bad data
				if(ExcelParsing.isEmptyCell(nextRow.getCell(colIndex))) {
					continue;
				}
				
				Object propValue = ExcelParsing.getCell(nextRow.getCell(colIndex));
				if(propValue instanceof SemossDate) {
					propValue = ((SemossDate) propValue).getDate();
				}
				propHash.put(propName, propValue);
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
	public void loadMatrixSheet(String sheetToLoad, Workbook workbook) {
		Sheet lSheet = workbook.getSheet(sheetToLoad);
		int lastRow = lSheet.getLastRowNum();
		logger.info("Number of Rows: " + lastRow);

		// Get the first row to get column names
		Row row = lSheet.getRow(0);
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
				Row nextRow = lSheet.getRow(rowIndex);
				// get the name subject instance
				String instanceSubjectName = nextRow.getCell(1).getStringCellValue();
				// see what relationships are mapped between subject instances and object instances
				for (int colIndex = 2; colIndex <= lastColumn; colIndex++) {
					String instanceObjectName = objectInstanceArray.get(colIndex - 2);
					Hashtable<String, Object> propHash = new Hashtable<String, Object>();
					// store value in cell between instance subject and object in current iteration of loop
					Cell matrixContent = nextRow.getCell(colIndex);
					// if any value in cell, there should be a mapping
					if (matrixContent != null) {
						if (propExists) {
							if (matrixContent.getCellType() == Cell.CELL_TYPE_NUMERIC) {
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
		ImportOptions options = new ImportOptions();
		// DATABASE WILL BE WRITTEN WHERE YOUR DB FOLDER IS IN A FOLDER WITH THE ENGINE NAME
		// SMSS file will not be created at the moment.. will add shortly
		String fileNames = "C:\\Users\\mahkhalil\\Desktop\\Medical_Devices_Data.xlsx";options.setFileLocation(fileNames);
		String smssLocation = "";options.setSMSSLocation(smssLocation);
		String engineName = "test";options.setDbName(engineName);
		String customBase = "http://semoss.org/ontologies";options.setBaseUrl(customBase);
		String owlFile = DIHelper.getInstance().getProperty("BaseFolder") + "\\db\\" + engineName + "\\" + engineName + "_OWL.OWL";options.setOwlFileLocation(owlFile);
		RdbmsTypeEnum dbType = RdbmsTypeEnum.H2_DB;
		options.setRDBMSDriverType(dbType);
		options.setAllowDuplicates(false);

		//reader.importFileWithOutConnectionRDBMS(smssLocation, engineName, fileNames, customBase, owlFile, dbType, false);
		reader.importFileWithOutConnectionRDBMS(options);
	}

}
