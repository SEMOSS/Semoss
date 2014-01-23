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
package prerna.poi.main;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.memory.MemoryStore;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * Loading data into SEMOSS using Microsoft Excel Loading Sheet files
 */
public class POIReader {

	Logger logger = Logger.getLogger(getClass());

	Properties rdfMap = new Properties(); // rdf mapping DBCM Prop
	Properties bdProp = new Properties(); // properties for big data
	public static String CONTAINS = "Contains";
	public SailConnection sc;
	Sail bdSail;
	ValueFactory vf;
	String customBaseURI = "";
	Hashtable<String, String> baseRelationsTypeHash = new Hashtable<String, String>(); // base relations type
	Hashtable<String, String> baseRelationsSubjectHash = new Hashtable<String, String>(); // base relations object URIs
	public Hashtable<String, String> createdURIsHash = new Hashtable<String, String>(); // new object URIs
	public Hashtable<String, String> createdBaseURIsHash = new Hashtable<String, String>(); // new object URIs
	public Hashtable<String, String> createdRelURIsHash = new Hashtable<String, String>(); // new relationship URIs
	public Hashtable<String, String> createdBaseRelURIsHash = new Hashtable<String, String>(); // new relationship URIs
	public String basePropURI = "";
	public POIReader importReader;
	public String semossURI;
	// OWL variables
	RepositoryConnection rcOWL;
	ValueFactory vfOWL;
	SailConnection scOWL;
	String owlFile;

	/**
	 * The main method is never called within SEMOSS
	 * Used to load data without having to start SEMOSS
	 * User must specify location of all files manually inside the method
	 * @param args String[]
	 */
	public static void main(String[] args) throws Exception {
		// try to load the file and see the worksheets

		String workingDir = System.getProperty("user.dir");
		String propFile = ""; //DO NOT EDIT HERE---it is now specified in the loops below, depending on what db you are loading
		String bdPropFile = ""; //DO NOT EDIT HERE---it is now specified in the loops below, depending on what db you are loading
		ArrayList<String> files = new ArrayList<String>();

		// UPDATE THESE FOUR THINGS TO SPECIFY WHAT YOU WANT/HOW TO LOAD::::::::::::::::::::::::::::::::::
		String customBase = "http://health.mil/ontologies";
		boolean runCoreLoadingSheets = false;
		boolean runFinancialLoadingSheets = true;
		boolean runCustomLoadSheets = false;


		if(runCoreLoadingSheets){
			propFile = workingDir + "/RDF_Map.prop";
			bdPropFile = workingDir + "/db/coreTest.smss";

			String coreFile1 = workingDir + "/Version_5main.xlsm";
			//files.add(coreFile1);
			String coreFile2 = workingDir + "/Version_5p2.xlsx";
			//files.add(coreFile2);
			String coreFile3 = workingDir + "/Version_5ser.xlsx";
			//files.add(coreFile3);
			String coreFile4 = workingDir + "/Version_5req.xlsx";
			//files.add(coreFile4);
			String coreFile5 = workingDir + "/DataElementsLoadSheet.xlsx";
			files.add(coreFile5);	
			String coreFile6 = workingDir + "/TransitionCostLoadingSheetsv2.xlsx";
			files.add(coreFile6);
			String coreFile7 = workingDir + "/HWSW loadsheet.xlsx";
			files.add(coreFile7);
		}

		if(runFinancialLoadingSheets){
			propFile = workingDir + "/RDF_Map.prop";
			bdPropFile = workingDir + "/db/TAP_Cost_Data.smss";

			String financialFile1 = workingDir + "/LoadingSheets1.xlsx";
			files.add(financialFile1);
			String financialFile2 = workingDir + "/TransitionCostLoadingSheetsv2.xlsx";
			//files.add(financialFile2);
			String financialFile3 = workingDir + "/Site_HWSW.xlsx";
			//files.add(financialFile3);
			String financialFile4 = workingDir + "/AncillaryGLItems.xlsx";
			//files.add(financialFile4);
			String financialFile5 = workingDir + "/SDLCLoadingSheets.xlsx";
			//files.add(financialFile5);
			String financialFile6 = workingDir +"/PFFinancialLoadingSheets2.xlsx";
			//files.add(financialFile6);
			String financialFile7 = workingDir + "/Version_5p2.xlsx";
			//files.add(financialFile7);
		}

		if(runCustomLoadSheets){
			propFile = workingDir + "/RDF_Map.prop";
			bdPropFile = workingDir + "/db/financial/CostData.Properties";

			String fileName1 = workingDir + "/CustomSheet.xlsx";
			files.add(fileName1);
		}


		POIReader reader = new POIReader();
		if(customBase!=null) {
			reader.customBaseURI = customBase;
		}
		reader.semossURI= "http://semoss.org/ontologies";
		reader.loadBDProperties(bdPropFile);
		reader.openDB();
		reader.openOWLWithOutConnection();
		if(reader.customBaseURI == null)
			reader.loadProperties(propFile);
		for(String fileName : files){
			reader.importFile(fileName);
		}
		reader.createBaseRelations();
		reader.closeDB();
	}

	/**
	 * Load data into SEMOSS into an existing database
	 * @param engineName 	String grabbed from the user interface specifying which database to add the data
	 * @param fileNames 	Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase 	String grabbed from the user interface that is used as the URI base for all instances
	 * @param customMap 	Absolute path that determines the location of the current db map file for the data
	 * @param owlFile 		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 */
	public void importFileWithConnection(String engineName, String fileNames, String customBase, String customMap, String owlFile) throws Exception 
	{
		String[] files = fileNames.split(";");
		this.semossURI = (String) DIHelper.getInstance().getLocalProp(Constants.SEMOSS_URI);
		this.owlFile = owlFile; 
		// load map file for existing db
		if(!customMap.equals(""))
		{
			loadProperties(customMap);
		}
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
		BigDataEngine bigEngine= (BigDataEngine) engine;
		openOWLWithConnection(engine);
		semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		if(!customBase.equals(""))
		{
			customBaseURI = customBase;
		}
		bdSail = bigEngine.bdSail;
		sc = bigEngine.sc;
		vf = bigEngine.vf;
		for(String fileName : files)
		{
			importFile(fileName);
		}
		createBaseRelations();
		bigEngine.infer();
	}

	/**
	 * Loading data into SEMOSS to create a new database
	 * @param dbName 		String grabbed from the user interface that would be used as the name for the database
	 * @param fileNames		Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase	String grabbed from the user interface that is used as the URI base for all instances 
	 * @param customMap		Absolute path that determines the location of a custom map file for the data
	 * @param owlFile		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 */
	public void importFileWithOutConnection(String dbName, String fileNames, String customBase, String customMap, String owlFile) throws Exception 
	{
		//convert fileNames to array list of files-------all fileNames separated by ";" as defined in FileBrowseListener
		String[] files = fileNames.split(";");
		this.owlFile = owlFile; 
		// load map file for db if user wants to use specific URIs
		if(!customMap.equals("")) 
		{
			loadProperties(customMap);
		}
		String bdPropFile = dbName;
		this.semossURI = (String) DIHelper.getInstance().getLocalProp(Constants.SEMOSS_URI);
		if(!customBase.equals(""))
		{
			customBaseURI = customBase;
		}
		loadBDProperties(bdPropFile);
		openDB();
		openOWLWithOutConnection();
		semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		//if user selected a map, load just as before--using the prop file to discover Excel->URI translation
		for(String fileName : files){
			importFile(fileName);
		}
		createBaseRelations();
		closeDB();
	}

	/**
	 * Load subclassing information into the db and the owl file
	 * Requires the data to be in specific excel tab labeled "Subclass", with Parent nodes in the first column and child nodes in the second column
	 * @param subclassSheet		Excel sheet with the subclassing information
	 */
	private void createSubClassing(XSSFSheet subclassSheet) throws Exception {
		// URI for sublcass
		String pred = Constants.SUBCLASS_URI;

		// check parent and child nodes in correct position
		XSSFRow row = subclassSheet.getRow(0);
		String parentNode = row.getCell(0).toString();
		String childNode = row.getCell(1).toString();
		// check to make sure parent column is in the correct column
		if (!parentNode.equalsIgnoreCase("Parent")){
			JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
			JOptionPane.showMessageDialog(playPane, "<html>Error with Subclass Sheet.<br>Error in parent node column.</html>");
			throw new Exception();
		}
		// check to make sure child column is in the correct column
		if(!childNode.equalsIgnoreCase("Child")){
			JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
			JOptionPane.showMessageDialog(playPane, "<html>Error with Subclass Sheet.<br>Error in child node column.</html>");
			throw new Exception();
		}
		// loop through and create all the triples for subclassing
		int lastRow = subclassSheet.getLastRowNum();
		for (int i = 1; i <= lastRow; i++){
			row = subclassSheet.getRow(i);
			parentNode = semossURI + "/" + Constants.DEFAULT_NODE_CLASS + "/" + row.getCell(0).toString();
			childNode = semossURI + "/" + Constants.DEFAULT_NODE_CLASS + "/" + row.getCell(1).toString();
			// add triples to engine
			createStatement(vf.createURI(childNode), vf.createURI(pred), vf.createURI(parentNode));
			// add triples to OWL
			scOWL.addStatement(vf.createURI(childNode), vf.createURI(pred), vf.createURI(parentNode));
		}
		scOWL.commit();
	}

	/**
	 * Creates all base relationships in the metamodel to add into the database and creates the OWL file
	 */
	public void createBaseRelations() throws Exception{
		if(baseRelationsTypeHash.size()>0) 
		{
			// necessary triple saying Concept is a type of Class
			String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
			String pred = RDF.TYPE.stringValue();
			String obj = Constants.CLASS_URI;
			createStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));
			scOWL.addStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));
			// necessary triple saying Relation is a type of Property
			sub =  semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
			pred = RDF.TYPE.stringValue();
			obj = Constants.DEFAULT_PROPERTY_URI;
			createStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));
			scOWL.addStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));

			if(!basePropURI.equals(""))
			{
				createStatement(vf.createURI(basePropURI), vf.createURI(Constants.SUBPROPERTY_URI), vf.createURI(basePropURI));
				scOWL.addStatement(vf.createURI(basePropURI), vf.createURI(Constants.SUBPROPERTY_URI), vf.createURI(basePropURI));
			}

			Iterator<String> baseHashIt = baseRelationsTypeHash.keySet().iterator();
			//now add all of the base relations that have been stored in the hash.
			while(baseHashIt.hasNext()){
				String subjectInstance = baseHashIt.next().toString();
				String objectInstance = baseRelationsTypeHash.get(subjectInstance).toString();

				// predicate depends on whether its a relation or a node
				String predicate = "";
				if(objectInstance.equals(Constants.DEFAULT_NODE_CLASS))
				{
					predicate = Constants.SUBCLASS_URI;
				}
				else if (objectInstance.equals(Constants.DEFAULT_RELATION_CLASS)){
					predicate = Constants.SUBPROPERTY_URI;
				}

				// convert instances to URIs
				String subject = baseRelationsSubjectHash.get(subjectInstance).toString();
				String object = semossURI + "/" + objectInstance;

				// create the statement now
				createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
				scOWL.addStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
				logger.info(subject +" "+ predicate +" "+ object);
			}
		}

		scOWL.commit();

		// TODO DELETE THIS CODE ONCE ALL DATABASES HAVE BEEN CREATED WITH OWL FILES, DO NOT PERFORM CHECK, JUST WRITE OWLFILE
		// export OWL File
		if(owlFile == null){
			JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
			JOptionPane.showMessageDialog(playPane, "<html>No OWL File found for existing database.<br> New Base Relations will not be stored </html>");
			throw new Exception();
		}
		else{
			FileWriter fWrite = new FileWriter(owlFile);
			RDFXMLPrettyWriter owlWriter  = new RDFXMLPrettyWriter(fWrite); 
			rcOWL.export(owlWriter);
			fWrite.close();
			owlWriter.close();	
		}

		scOWL.close();
		rcOWL.close();
	}

	/**
	 * Load the excel workbook, determine which sheets to load in workbook from the Loader tab
	 * @param fileName		String containing the absolute path to the excel workbook to load
	 */
	public void importFile(String fileName) throws Exception {

		XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(fileName));
		// load the Loader tab to determine which sheets to load
		XSSFSheet lSheet = workbook.getSheet("Loader");

		// check if user is loading subclassing relationships
		XSSFSheet subclassSheet = workbook.getSheet("Subclass");
		if (subclassSheet != null){
			createSubClassing(subclassSheet);
		}

		// determine number of sheets to load
		int lastRow = lSheet.getLastRowNum();
		// first sheet name in second row
		for (int rIndex = 1; rIndex <= lastRow; rIndex++) 
		{
			XSSFRow row = lSheet.getRow(rIndex);
			// check to make sure cell is not null
			XSSFCell cell = row.getCell(0);
			if(cell != null)
			{
				// get the name of the sheet
				String sheetToLoad = row.getCell(0).getStringCellValue();
				// determine the type of load
				String loadTypeName = row.getCell(1).getStringCellValue();
				if (!sheetToLoad.isEmpty() && !loadTypeName.isEmpty()) 
				{
					logger.debug("Cell Content is " + sheetToLoad);
					// this is a relationship
					if (loadTypeName.contains("Matrix")) 
					{
						loadMatrixSheet(sheetToLoad, workbook);
						sc.commit();
					} 
					else 
					{
						loadSheet(sheetToLoad, workbook);
						sc.commit();
					}
				}
			}
		}
	}

	/**
	 * Load specific sheet in workbook
	 * @param sheetToLoad 	String containing the name of the sheet to load
	 * @param workbook		XSSFWorkbook containing the sheet to load
	 */
	public void loadSheet(String sheetToLoad, XSSFWorkbook workbook) throws Exception{

		XSSFSheet lSheet = workbook.getSheet(sheetToLoad);
		logger.info("Loading Sheet: " + sheetToLoad);
		int lastRow = lSheet.getLastRowNum()+1;

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
		for (int colIndex = currentColumn + 1; colIndex < row.getLastCellNum(); colIndex++){
			// add property name to vector
			propNames.addElement(row.getCell(colIndex).getStringCellValue());
			lastColumn = colIndex;
		}
		logger.info("Number of Columns: " + (lastColumn+1));

		// processing starts
		try {
			logger.info("Number of Rows: " + lastRow);
			for (int rowIndex = 1; rowIndex < lastRow; rowIndex++) {
				// first cell is the name of relationship
				XSSFRow nextRow = lSheet.getRow(rowIndex);

				// get the name of the relationship
				if (rowIndex == 1)
					relName = nextRow.getCell(0).getStringCellValue();

				// get the name of the subject instance node
				if (nextRow.getCell(1) != null	&& nextRow.getCell(1).getCellType() != XSSFCell.CELL_TYPE_BLANK){
					nextRow.getCell(1).setCellType(Cell.CELL_TYPE_STRING);					
				}
				String instanceSubjectNode = nextRow.getCell(1).getStringCellValue();
				// get the name of the object instance node if relationship
				String instanceObjectNode = "";
				int startCol = 1;
				int offset = 1;
				if (sheetType.equalsIgnoreCase("Relation")) {
					nextRow.getCell(2).setCellType(Cell.CELL_TYPE_STRING);
					instanceObjectNode = nextRow.getCell(2).getStringCellValue();
					startCol++;
					offset++;
				}

				// add properties to propHash
				Hashtable<String, Object> propHash = new Hashtable<String, Object>();
				for (int colIndex = (startCol + 1); colIndex < nextRow.getLastCellNum(); colIndex++) {
					if(propNames.size() <= (colIndex-offset)) {
						continue;
					}
					String propName = propNames.elementAt(colIndex - offset).toString();
					String propValue = "";
					if (nextRow.getCell(colIndex) == null || nextRow.getCell(colIndex).getCellType() == XSSFCell.CELL_TYPE_BLANK || nextRow.getCell(colIndex).toString().isEmpty()) {
						continue;
					}
					if (nextRow.getCell(colIndex).getCellType() == XSSFCell.CELL_TYPE_NUMERIC) {
						if(DateUtil.isCellDateFormatted(nextRow.getCell(colIndex))){
							Date date = (Date) nextRow.getCell(colIndex).getDateCellValue();
							propHash.put(propName, date);
						}
						else{
							Double dbl = new Double(nextRow.getCell(colIndex).getNumericCellValue());
							propHash.put(propName, dbl);
						}
					} else {
						propValue = nextRow.getCell(colIndex).getStringCellValue();
						propHash.put(propName, propValue);
					}
				}

				if (sheetType.equalsIgnoreCase("Relation")) 
				{
					// adjust indexing since first row in java starts at 0
					logger.info("Processing Relationship Sheet: " + sheetToLoad + ", Row: " + (rowIndex+1));
					processRelationships(subjectNode, objectNode, instanceSubjectNode, instanceObjectNode, relName, propHash);
				} 
				else 
				{
					// adjust indexing since first row in java starts at 0
					logger.info("Processing Node Property Sheet: " + sheetToLoad + ", Row: " + (rowIndex+1));
					processNodeProperties(subjectNode, instanceSubjectNode, propHash);
				}
				if(rowIndex == (lastRow-1)){
					logger.info("Done processing: " + sheetToLoad);	
				}
			}
		} finally {
		}
	}

	/**
	 * Create and add all triples associated with relationship tabs
	 * @param subjectNodeType 			String containing the subject node type
	 * @param objectNodeType 			String containing the object node type
	 * @param instanceSubjectName 		String containing the name of the subject instance
	 * @param instanceObjectName	 	String containing the name of the object instance
	 * @param relName 					String containing the name of the relationship between the subject and object
	 * @param propHash 					Hashtable that contains all properties
	 */
	public void processRelationships(String subjectNodeType, String objectNodeType, String instanceSubjectName, String instanceObjectName, String relName, Hashtable<String, Object> propHash) throws Exception{
		// cellCounter used to determine which column currently processing
		int cellCounter = 'B';
		instanceSubjectName = Utility.cleanString(instanceSubjectName, true);
		instanceObjectName = Utility.cleanString(instanceObjectName, true);

		// Generate URI for subject node at the instance and base level
		String subjectInstanceBaseURI = "";
		String subjectSemossBaseURI = "";
		// check to see if user specified URI in custom map file
		if(rdfMap.containsKey(subjectNodeType+Constants.CLASS))
		{
			subjectSemossBaseURI = rdfMap.getProperty(subjectNodeType+Constants.CLASS);
		}
		// if no user specified URI, use generic SEMOSS URI
		else
		{
			subjectSemossBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subjectNodeType;
		}
		// check to see if user specified URI in custom map file
		if(rdfMap.containsKey(subjectNodeType))
		{
			subjectInstanceBaseURI = rdfMap.getProperty(subjectNodeType);
		}
		// if no user specified URI, use generic customBaseURI
		else 
		{
			subjectInstanceBaseURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subjectNodeType;
		}

		baseRelationsTypeHash.put(subjectNodeType, Constants.DEFAULT_NODE_CLASS);
		baseRelationsSubjectHash.put(subjectNodeType, subjectSemossBaseURI);
		createdURIsHash.put(subjectNodeType, subjectInstanceBaseURI);
		createdBaseURIsHash.put(subjectNodeType+Constants.CLASS, subjectSemossBaseURI);

		// generate base URIs for object node at instance and semoss level
		String objectInstanceBaseURI = "";
		String objectSemossBaseURI = "";
		// check to see if user specified URI in custom map file
		if(rdfMap.containsKey(objectNodeType+Constants.CLASS))
		{
			objectSemossBaseURI = rdfMap.getProperty(objectNodeType+Constants.CLASS);
		}
		// if no user specified URI, use generic SEMOSS URI
		else
		{
			objectSemossBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ objectNodeType;
		}
		// check to see if user specified URI in custom map file
		if(rdfMap.containsKey(objectNodeType)) 
		{
			objectInstanceBaseURI = rdfMap.getProperty(objectNodeType);
		}
		// if no user specified URI, use generic customBaseURI
		else 
		{
			objectInstanceBaseURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ objectNodeType;
		}

		baseRelationsTypeHash.put(objectNodeType, Constants.DEFAULT_NODE_CLASS);
		baseRelationsSubjectHash.put(objectNodeType, objectSemossBaseURI);
		createdURIsHash.put(objectNodeType, objectInstanceBaseURI);
		createdBaseURIsHash.put(objectNodeType+Constants.CLASS, objectSemossBaseURI);

		// create the full URI for the subject instance
		// add type and label triples to database
		logger.info("Processing Node-Type Column: " + ((char) cellCounter)); 
		String subjectNodeURI = subjectInstanceBaseURI + "/" + instanceSubjectName;
		createStatement(vf.createURI(subjectNodeURI), RDF.TYPE, vf.createURI(subjectSemossBaseURI));
		createStatement(vf.createURI(subjectNodeURI), RDFS.LABEL, vf.createLiteral(instanceSubjectName));
		cellCounter++;
		
		// create the full URI for the object instance
		// add type and label triples to database
		logger.info("Processing Node-Type Column: " + ((char) cellCounter)); 
		String objectNodeURI = objectInstanceBaseURI + "/" + instanceObjectName;
		createStatement(vf.createURI(objectNodeURI), RDF.TYPE, vf.createURI(objectSemossBaseURI));
		createStatement(vf.createURI(objectNodeURI), RDFS.LABEL, vf.createLiteral(instanceObjectName));
		cellCounter++;
		
		// generate URIs for the relationship
		relName = Utility.cleanString(relName, true);
		String relInstanceBaseURI = "";
		String relSemossBaseURI = "";
		// check to see if user specified URI in custom map file
		if(rdfMap.containsKey(subjectNodeType + "_"+ relName + "_" + objectNodeType+Constants.CLASS)) 
		{
			relSemossBaseURI = rdfMap.getProperty(subjectNodeType + "_"+ relName + "_" + objectNodeType+Constants.CLASS);
		}
		// if no user specified URI, use generic SEMOSS URI
		else
		{
			relSemossBaseURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
		}
		// check to see if user specified URI in custom map file
		if(rdfMap.containsKey(subjectNodeType + "_"+ relName + "_" + objectNodeType)) 
		{
			relInstanceBaseURI = rdfMap.getProperty(subjectNodeType + "_"+ relName + "_" + objectNodeType);
		}
		// if no user specified URI, use generic customBaseURI
		else 
		{
			relInstanceBaseURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
		}

		baseRelationsTypeHash.put(relName, Constants.DEFAULT_RELATION_CLASS);
		baseRelationsSubjectHash.put(relName, relSemossBaseURI);
		createdRelURIsHash.put(subjectNodeType + "_"+ relName + "_" + objectNodeType, relInstanceBaseURI);
		createdBaseRelURIsHash.put(subjectNodeType + "_"+ relName + "_" + objectNodeType +Constants.CLASS, relSemossBaseURI);

		// create instance value of relationship and add instance relationship, subproperty, and label triples
		String instanceRelURI = relInstanceBaseURI + "/" + instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName;
		logger.info("Processing Relationship Between Nodes"); 
		createStatement(vf.createURI(instanceRelURI), RDFS.SUBPROPERTYOF, vf.createURI(relSemossBaseURI));
		createStatement(vf.createURI(instanceRelURI), RDFS.LABEL, vf.createLiteral(instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName));
		createStatement(vf.createURI(subjectNodeURI), vf.createURI(instanceRelURI), vf.createURI(objectNodeURI));

		// add all relationship properties
		Enumeration<String> propKeys = propHash.keys();
		if(basePropURI.equals(""))
		{
			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		}
		// add property triple based on data type of property
		while(propKeys.hasMoreElements()) 
		{
			String key = propKeys.nextElement().toString();
			String propURI = basePropURI + "/" + key;
			logger.info("Processing Property Name");
			createStatement(vf.createURI(propURI), RDF.TYPE, vf.createURI(basePropURI));	
			if(propHash.get(key).getClass() == new Double(1).getClass())
			{
				Double value = (Double) propHash.get(key);
				logger.info("Processing Property Column: " + ((char) cellCounter)); 
				createStatement(vf.createURI(instanceRelURI), vf.createURI(propURI), vf.createLiteral(value.doubleValue()));
			}
			else if(propHash.get(key).getClass() == new Date(1).getClass())
			{
				Date value = (Date)propHash.get(key);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(value);
				URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
				logger.info("Processing Property Column: " + ((char) cellCounter)); 
				createStatement(vf.createURI(instanceRelURI), vf.createURI(propURI), vf.createLiteral(date, datatype));
			}
			else
			{
				String value = propHash.get(key).toString();
				String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");
				logger.info("Processing Property Column: " + ((char) cellCounter)); 
				createStatement(vf.createURI(instanceRelURI), vf.createURI(propURI), vf.createLiteral(cleanValue));
			}
			cellCounter++;
		}
	}

	/**
	 * Create and add triples associated with node property tabs
	 * @param subjectNodeType 			String containing the subject node type
	 * @param instanceSubjectName 		String containing the name of the subject instance
	 * @param propHash 					Hashtable that contains all properties
	 */
	public void processNodeProperties(String subjectNodeType, String instanceSubjectName, Hashtable<String, Object> propHash) throws Exception{
		// cellCounter used to determine which column currently processing
		int cellCounter = 'B';
		instanceSubjectName = Utility.cleanString(instanceSubjectName, true);

		// Generate URI for subject node at the instance and base level
		String subjectInstanceBaseURI = "";
		String subjectSemossBaseURI = "";
		// check to see if user specified URI in custom map file
		if(rdfMap.containsKey(subjectNodeType+Constants.CLASS))
		{
			subjectSemossBaseURI = rdfMap.getProperty(subjectNodeType+Constants.CLASS);
		}
		// if no user specified URI, use generic SEMOSS URI
		else
		{
			subjectSemossBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subjectNodeType;
		}
		// check to see if user specified URI in custom map file
		if(rdfMap.containsKey(subjectNodeType))
		{
			subjectInstanceBaseURI = rdfMap.getProperty(subjectNodeType);
		}
		// if no user specified URI, use generic customBaseURI
		else 
		{
			subjectInstanceBaseURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subjectNodeType;
		}

		baseRelationsTypeHash.put(subjectNodeType, Constants.DEFAULT_NODE_CLASS);
		baseRelationsSubjectHash.put(subjectNodeType, subjectSemossBaseURI);
		createdURIsHash.put(subjectNodeType, subjectInstanceBaseURI);
		createdBaseURIsHash.put(subjectNodeType+Constants.CLASS, subjectSemossBaseURI);

		// create the full URI for the subject instance
		// add type and label triples to database
		String subjectNodeURI = subjectInstanceBaseURI + "/" + instanceSubjectName;
		logger.info("Processing Node-Type Column: " + ((char) cellCounter)); 
		createStatement(vf.createURI(subjectNodeURI), RDF.TYPE, vf.createURI(subjectSemossBaseURI));
		createStatement(vf.createURI(subjectNodeURI), RDFS.LABEL, vf.createLiteral(instanceSubjectName));
		cellCounter++;
		
		// add all relationship properties
		Enumeration<String> propKeys = propHash.keys();
		if(basePropURI.equals(""))
		{
			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		}
		// add property triple based on data type of property
		while (propKeys.hasMoreElements()) 
		{
			String key = propKeys.nextElement().toString();
			String propInstanceURI = basePropURI + "/" + key;
			logger.info("Processing Property Name");
			createStatement(vf.createURI(propInstanceURI), RDF.TYPE, vf.createURI(basePropURI));
			if(propHash.get(key).getClass() == new Double(1).getClass())
			{
				logger.info("Processing Property Column: " + ((char) cellCounter)); 
				Double value = (Double) propHash.get(key);
				createStatement(vf.createURI(subjectNodeURI), vf.createURI(propInstanceURI), vf.createLiteral(value.doubleValue()));
			}
			else if(propHash.get(key).getClass() == new Date(1).getClass())
			{
				Date value = (Date)propHash.get(key);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(value);
				URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
				logger.info("Processing Property Column: " + ((char) cellCounter)); 
				createStatement(vf.createURI(subjectNodeURI), vf.createURI(propInstanceURI), vf.createLiteral(date, datatype));
			}
			else
			{
				String value = propHash.get(key).toString();
				if(value.equals(Constants.PROCESS_CURRENT_DATE)){
					logger.info("Processing Property Column: " + ((char) cellCounter)); 
					insertCurrentDate(propInstanceURI, basePropURI, subjectNodeURI);
				}
				else if(value.equals(Constants.PROCESS_CURRENT_USER)){
					logger.info("Processing Property Column: " + ((char) cellCounter)); 
					insertCurrentUser(propInstanceURI, basePropURI, subjectNodeURI);
				}
				else{
					logger.info("Processing Property Column: " + ((char) cellCounter)); 
					String cleanValue = Utility.cleanString(value, true);
					createStatement(vf.createURI(subjectNodeURI), vf.createURI(propInstanceURI), vf.createLiteral(cleanValue));
				}
			}
			cellCounter++;
		}
	}

	/**
	 * Insert the current user as a property onto a node if property is "PROCESS_CURRENT_USER"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 */
	private void insertCurrentUser(String propURI, String basePropURI, String subjectNodeURI){
		String cleanValue = System.getProperty("user.name");
		try{
			createStatement(vf.createURI(propURI), RDF.TYPE, vf.createURI(basePropURI));				
			createStatement(vf.createURI(subjectNodeURI), vf.createURI(propURI), vf.createLiteral(cleanValue));
		} catch (Exception e) {
			logger.error(e);
		}	
	}

	/**
	 * Insert the current date as a property onto a node if property is "PROCESS_CURRENT_DATE"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 */
	private void insertCurrentDate(String propInstanceURI, String basePropURI, String subjectNodeURI){
		Date dValue = new Date();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String date = df.format(dValue);
		try {
			createStatement(vf.createURI(propInstanceURI), RDF.TYPE, vf.createURI(basePropURI));
			URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
			createStatement(vf.createURI(subjectNodeURI), vf.createURI(propInstanceURI), vf.createLiteral(date, datatype));
		} catch (Exception e) {
			logger.error(e);
		}	
	}

	/**
	 * Load excel sheet in matrix format
	 * @param sheetToLoad 	String containing the name of the excel sheet to load
	 * @param workbook		XSSFWorkbook containing the name of the excel workbook
	 */
	public void loadMatrixSheet(String sheetToLoad, XSSFWorkbook workbook) throws Exception
	{
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
		StringTokenizer tokenProperties = new StringTokenizer(nodeMap,"@");
		String triple = tokenProperties.nextToken();
		if(tokenProperties.hasMoreTokens()){
			propertyName = tokenProperties.nextToken();
			propExists = true;
		}

		StringTokenizer tokenTriple = new StringTokenizer(triple, "_");
		String subjectNodeType = tokenTriple.nextToken();
		if(sheetType.equalsIgnoreCase("Relation")) {
			relName = tokenTriple.nextToken();
			objectNodeType = tokenTriple.nextToken();
		}

		// determine object instance names for the relationship
		ArrayList<String> objectInstanceArray = new ArrayList<String>();
		int lastColumn = 0;
		for (int colIndex = 2; colIndex < row.getLastCellNum(); colIndex++){
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
				for(int colIndex = 2; colIndex <= lastColumn; colIndex++){
					String instanceObjectName = objectInstanceArray.get(colIndex-2);
					Hashtable<String, Object> propHash = new Hashtable<String, Object>();
					// store value in cell between instance subject and object in current iteration of loop
					XSSFCell matrixContent = nextRow.getCell(colIndex);
					// if any value in cell, there should be a mapping
					if(matrixContent!=null)
					{
						if(propExists){
							if(matrixContent.getCellType() == XSSFCell.CELL_TYPE_NUMERIC){
								if(DateUtil.isCellDateFormatted(matrixContent)){
									propHash.put(propertyName, (Date) matrixContent.getDateCellValue());
									mapExists = true;
								}
								else{
									propHash.put(propertyName, new Double(matrixContent.getNumericCellValue()));
									mapExists = true;
								}
							}
							else{
								// if not numeric, assume it is a string and check to make sure it is not empty
								if(!matrixContent.getStringCellValue().isEmpty()){
									propHash.put(propertyName,matrixContent.getStringCellValue());
									mapExists = true;
								}
							}
						}
						else{
							mapExists = true;
						}
					}

					if (sheetType.equalsIgnoreCase("Relation") && mapExists)
					{
						logger.info("Processing" + sheetToLoad + " Row " + rowIndex + " Column " + colIndex);
						processRelationships(subjectNodeType, objectNodeType, instanceSubjectName, instanceObjectName, relName, propHash);
					}
					else
					{
						logger.info("Processing" + sheetToLoad + " Row " + rowIndex + " Column " + colIndex);
						processNodeProperties(subjectNodeType, instanceSubjectName, propHash);	
					}
				}
				logger.info(instanceSubjectName);
			}
		} finally {
			logger.info("Done processing: " + sheetToLoad);
		}
	}

	/**
	 * Creates and adds the triple into the repository connection
	 * @param subject		URI for the subject of the triple
	 * @param predicate		URI for the predicate of the triple
	 * @param object		Value for the object of the triple, this param is not a URI since objects can be literals and literals do not have URIs
	 */
	private void createStatement(URI subject, URI predicate, Value object) throws Exception
	{
		URI newSub;
		URI newPred;
		Value newObj;
		String subString;
		String predString;
		String objString;
		String sub = subject.stringValue().trim();
		String pred = predicate.stringValue().trim();

		subString = Utility.cleanString(sub, false);
		newSub = vf.createURI(subString);

		predString = Utility.cleanString(pred, false);
		newPred = vf.createURI(predString);

		if(object instanceof Literal) 
			newObj = object;
		else {
			objString = Utility.cleanString(object.stringValue(), false);
			newObj = vf.createURI(objString);
		}
		sc.addStatement(newSub, newPred, newObj);
	}

	/**
	 * Creates a repository connection to be put all the base relationship data to create the OWL file
	 */
	public void openOWLWithOutConnection() throws RepositoryException
	{
		Repository myRepository = new SailRepository(new MemoryStore());
		myRepository.initialize();
		rcOWL = myRepository.getConnection();
		scOWL = ((SailRepositoryConnection) rcOWL).getSailConnection();
		vfOWL = rcOWL.getValueFactory();
	}

	/**
	 * Creates a repository connection and puts all the existing base relationships to create an updated OWL file
	 * @param engine	The database engine used to get all the existing base relationships
	 */
	public void openOWLWithConnection(IEngine engine) throws RepositoryException
	{
		Repository myRepository = new SailRepository(new MemoryStore());
		myRepository.initialize();
		rcOWL = myRepository.getConnection();
		scOWL = ((SailRepositoryConnection) rcOWL).getSailConnection();
		vfOWL = rcOWL.getValueFactory();

		AbstractEngine baseRelEngine = ((AbstractEngine)engine).getBaseDataEngine();
		RepositoryConnection existingRC = ((RDFFileSesameEngine) baseRelEngine).getRc();
		// load pre-existing base data
		RepositoryResult<Statement> rcBase = existingRC.getStatements(null, null, null, false);
		List<Statement> rcBaseList = rcBase.asList();
		Iterator<Statement> iterator = rcBaseList.iterator();
		while(iterator.hasNext()){
			logger.info(iterator.next());
		}
		rcOWL.add(rcBaseList);		
	}

	/**
	 * Creates the database based on the engine properties 
	 */
	public void openDB() throws Exception 
	{
		// create database based on engine properties
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String fileName = baseFolder + "/" + bdProp.getProperty("com.bigdata.journal.AbstractJournal.file");
		bdProp.put("com.bigdata.journal.AbstractJournal.file", fileName);
		bdSail = new BigdataSail(bdProp);
		Repository repo = new BigdataSailRepository((BigdataSail) bdSail);
		repo.initialize();
		SailRepositoryConnection src = (SailRepositoryConnection) repo.getConnection();
		sc = src.getSailConnection();
		vf = bdSail.getValueFactory();
	}

	/**
	 * Loading user custom map to specify unique URIs 
	 * @param fileName 	String containing the path to the custom map file
	 */
	public void loadProperties(String fileName) throws Exception {
		rdfMap.load(new FileInputStream(fileName));
	}

	/**
	 * Loading engine properties in order to create the database 
	 * @param fileName 	String containing the fileName of the temp file that contains the information of the smss file
	 */
	public void loadBDProperties(String fileName) throws Exception
	{
		bdProp.load(new FileInputStream(fileName));

	}

	/**
	 * Close the database engine
	 */
	public void closeDB() throws Exception
	{
		sc.commit();
		InferenceEngine ie = ((BigdataSail)bdSail).getInferenceEngine();
		ie.computeClosure(null);
		sc.commit();
		sc.close();
		bdSail.shutDown();
	}

}
