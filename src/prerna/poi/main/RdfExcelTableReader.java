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
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openrdf.model.vocabulary.RDF;

import prerna.engine.api.IEngine;
import prerna.poi.main.helper.ImportOptions;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Loading data into SEMOSS using comma separated value (Excel) files
 */
public class RdfExcelTableReader extends AbstractFileReader {

	private static final Logger logger = LogManager.getLogger(RdfExcelTableReader.class.getName());

	private String propFile; // the file that serves as the property file
	private List<String> headerList;
	private FileInputStream poiReader;
	private XSSFWorkbook workbook; 

	private ArrayList<String> relationArrayList = new ArrayList<String>();
	private ArrayList<String> nodePropArrayList = new ArrayList<String>();
	private ArrayList<String> relPropArrayList = new ArrayList<String>();

	private int startRow = 1;
	private int maxRows = 100000;
	
	/**
	 * Loading data into SEMOSS to create a new database
	 * @param dbName 		String grabbed from the user interface that would be used as the name for the database
	 * @param fileNames		Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase	String grabbed from the user interface that is used as the URI base for all instances 
	 * @param customMap		
	 * @param owlFile		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 * @return 
	 * @throws IOException 
	 */
	public IEngine importFileWithOutConnection(ImportOptions options) throws IOException {
		
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
			for(int i = 0; i<files.length;i++)
			{
				String fileName = files[i];
				openExcelWorkbook(fileName);
				try {
					for(int j = 0; j < workbook.getNumberOfSheets(); j++)
					{
						XSSFSheet sheet = workbook.getSheetAt(j);
						XSSFRow headRow = sheet.getRow(0);
						headerList = new ArrayList<String>();
						for(int k = 0; k < headRow.getLastCellNum(); k++) {
							headerList.add(headRow.getCell(k).toString());
						}
						// Process your sheet here.
						// load the prop file for the Excel file
						if(propFileExist){
							propFile = headerList.get(headerList.size()-1);
							headerList.remove(headerList.size()-1);
							openProp(propFile);
						} else {
							rdfMap = rdfMapArr[i];
						}
						//TODO: same method used in other classes
						parseMetadata();
						// determine the type of data in each column of Excel file
//						processConceptRelationURIs(sheet);
//						processNodePropURIs(sheet);
//						processRelationPropURIs(sheet);
						processRelationShips(sheet);
					}
				} finally {
					closeExcelWorkbook();
				}
			}
			createBaseRelations();
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
			}
		}
		
		return engine;
	}

	public void parseMetadata() {
		if(rdfMap.get("RELATION") != null)
		{
			String relationNames = rdfMap.get("RELATION");
			StringTokenizer relationTokens = new StringTokenizer(relationNames, ";");
			relationArrayList = new ArrayList<String>();
			// process each relationship
			while(relationTokens.hasMoreElements())
			{
				String relation = relationTokens.nextToken();
				// just in case the end of the prop string is empty string or spaces
				if(!relation.contains("@"))
					continue;

				relationArrayList.add(relation);
			}
		}
		
		if(rdfMap.get("NODE_PROP") != null)
		{
			String nodePropNames = rdfMap.get("NODE_PROP");
			StringTokenizer nodePropTokens = new StringTokenizer(nodePropNames, ";");
			nodePropArrayList = new ArrayList<String>();
			if(basePropURI.equals("")){
				basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
			}
			while(nodePropTokens.hasMoreElements())
			{
				String relation = nodePropTokens.nextToken();
				// in case the end of the prop string is empty string or spaces
				if(!relation.contains("%"))
					continue;

				nodePropArrayList.add(relation);
			}
		}
		
		if(rdfMap.get("RELATION_PROP") != null)
		{
			String propNames = rdfMap.get("RELATION_PROP");
			StringTokenizer propTokens = new StringTokenizer(propNames, ";");
			relPropArrayList = new ArrayList<String>();
			if(basePropURI.equals("")){
				basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
			}
			while(propTokens.hasMoreElements())
			{
				String relation = propTokens.nextToken();
				//just in case the end of the prop string is empty string or spaces
				if(!relation.contains("%"))
					continue;

				relPropArrayList.add(relation);
			}
		}
	}
	
	/**
	 * Load data into SEMOSS into an existing database
	 * @param engineId 	String grabbed from the user interface specifying which database to add the data
	 * @param fileNames 	Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase 	String grabbed from the user interface that is used as the URI base for all instances
	 * @param customMap 	Absolute path specified in the Excel file that determines the location of the prop file for the data
	 * @param owlFile 		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 * @throws IOException 
	 */
	public void importFileWithConnection(ImportOptions options) throws IOException {
		
		String engineName = options.getDbName();
		String fileNames = options.getFileLocations();
		String customBase = options.getBaseUrl();
		String owlFile = options.getOwlFileLocation();
		logger.setLevel(Level.WARN);
		String[] files = prepareReader(fileNames, customBase, owlFile, engineName);
		openEngineWithConnection(engineName);
		
		try {
			for(int i = 0; i<files.length;i++) {
				String fileName = files[i];
				openExcelWorkbook(fileName);	
				for(int j = 0; j < workbook.getNumberOfSheets(); j++)
				{
					XSSFSheet sheet = workbook.getSheetAt(j);
					propFile = sheet.getRow(0).getCell(sheet.getRow(0).getLastCellNum()-1).toString();
					// Process your sheet here.
					// load the prop file for the Excel file
					if(propFileExist){
						openProp(propFile);
					} else {
						rdfMap = rdfMapArr[i];
					}
					// determine the type of data in each column of Excel file
//					processConceptRelationURIs(sheet);
//					processNodePropURIs(sheet);
//					processRelationPropURIs(sheet);
					processRelationShips(sheet);
				}
			}
		} finally {
			closeExcelWorkbook();
		}
		createBaseRelations();
		commitDB();
	}

	/**
	 * Create all the triples associated with the relationships specified in the prop file
	 * @throws EngineException 
	 * @throws FileReaderException 
	 */
	public void processRelationShips(XSSFSheet sheet) throws IOException
	{
		// overwrite this value if user specified the max rows to load
		if (rdfMap.get(sheet.getSheetName() + "_START_ROW") != null)
		{
			startRow =  Integer.parseInt(rdfMap.get(sheet.getSheetName() + "_START_ROW"));
		}
		// get all the relation
		// overwrite this value if user specified the max rows to load
		if (rdfMap.get(sheet.getSheetName() + "_END_ROW") != null)
		{
			maxRows =  Integer.parseInt(rdfMap.get(sheet.getSheetName() + "_END_ROW"));
		}
		XSSFRow excelHeaders = sheet.getRow(0);
		int numCols = excelHeaders.getLastCellNum();
		// for performance, move header to String[]
		String[] headers = new String[numCols];
		for(int i = 0; i < numCols; i++) {
			headers[i] = excelHeaders.getCell(i).getStringCellValue();
		}
		int numRows = sheet.getLastRowNum();
		for(int i = startRow; i < numRows && i < maxRows; i++ )
		{
			logger.info("Process line: " + i);
			// process all relationships in row
			XSSFRow currRow = sheet.getRow(i);
			for(int relIndex = 0; relIndex < relationArrayList.size(); relIndex++)
			{
				String relation = relationArrayList.get(relIndex);
				String[] strSplit = relation.split("@");
				// get the subject and object for triple (the two indexes)
				String sub = strSplit[0];
				String subject = "";
				String predicate = strSplit[1];
				String obj = strSplit[2];
				String object = "";

				// see if subject node URI exists in prop file
				if(rdfMap.containsKey(sub))
				{
					String userSub = rdfMap.get(sub).toString(); 
					subject = userSub.substring(userSub.lastIndexOf("/")+1);

					//subject = rdfMap.get(sub);
				}
				// if no user specified URI, use generic URI
				else
				{
					if(sub.contains("+"))
					{
						subject = processAutoConcat(sub);
					}
					else
					{
						subject = sub;
					}
				}
				// see if object node URI exists in prop file
				if(rdfMap.containsKey(obj))
				{
					String userObj = rdfMap.get(obj).toString(); 
					object = userObj.substring(userObj.lastIndexOf("/")+1);

					//object = rdfMap.get(obj);
				}
				// if no user specified URI, use generic URI
				else
				{
					if(obj.contains("+"))
					{
						object = processAutoConcat(obj);
					}
					else
					{
						object = obj;
					}
				}

				String subjectValue = createInstanceValue(sub, currRow, headers);
				String objectValue = createInstanceValue(obj, currRow, headers);
				if (subjectValue.isEmpty() || objectValue.isEmpty())
				{
					continue;
				}

				// look through all relationship properties for the specific relationship
				Hashtable<String, Object> propHash = new Hashtable<String, Object>();

				for(int relPropIndex = 0; relPropIndex < relPropArrayList.size(); relPropIndex++)
				{
					String relProp = relPropArrayList.get(relPropIndex);
					String[] relPropSplit = relProp.split("%");
					if(relPropSplit[0].equals(relation))
					{
						// loop through all properties on the relationship
						for(int z = 1; z < relPropSplit.length; z++)
						{			
							// add the necessary triples for the relationship property
							String prop = relPropSplit[z];
							String property = "";
							// see if property node URI exists in prop file
							if(rdfMap.containsKey(prop))
							{
								String userProp = rdfMap.get(prop).toString(); 
								property = userProp.substring(userProp.lastIndexOf("/")+1);
								//property = rdfMap.get(prop);
							}
							// if no user specified URI, use generic URI
							else
							{
								if(prop.contains("+"))
								{
									property = processAutoConcat(prop);
								}
								else
								{
									property = prop;
								}
							}
							propHash.put(property, createObject(prop, currRow, headers));
						}
					}
				}
				createRelationship(subject, object, subjectValue, objectValue, predicate, propHash);
			}

			// look through all node properties
			for(int relIndex = 0;relIndex<nodePropArrayList.size();relIndex++)
			{
				Hashtable<String, Object> nodePropHash = new Hashtable<String, Object>();
				String relation = nodePropArrayList.get(relIndex);
				String[] strSplit = relation.split("%");
				// get the subject (the first index) and objects for triple
				String sub = strSplit[0];
				String subject = "";
				// see if subject node URI exists in prop file
				if(rdfMap.containsKey(sub))
				{
					String userSub = rdfMap.get(sub).toString(); 
					subject = userSub.substring(userSub.lastIndexOf("/")+1);

					//subject = rdfMap.get(sub);
				}
				// if no user specified URI, use generic URI
				else
				{	
					if(sub.contains("+"))
					{
						subject = processAutoConcat(sub);
					}
					else
					{
						subject = sub;
					}
				}
				String subjectValue = createInstanceValue(sub,currRow, headers);
				// loop through all properties on the node
				for(int b = 1; b < strSplit.length; b++)
				{
					String prop = strSplit[b];
					String property = "";
					// see if property node URI exists in prop file
					if(rdfMap.containsKey(prop))
					{
						String userProp = rdfMap.get(prop).toString(); 
						property = userProp.substring(userProp.lastIndexOf("/")+1);

						//property = rdfMap.get(prop);
					}
					// if no user specified URI, use generic URI
					else
					{
						if(prop.contains("+"))
						{
							property = processAutoConcat(prop);
						}
						else
						{
							property = prop;
						}
					}
					String propValue = createInstanceValue(prop,currRow, headers);
					if (subjectValue.isEmpty() || propValue.isEmpty())
					{
						continue;
					}
					nodePropHash.put(property, createObject(prop, currRow, headers));
				}
				addNodeProperties(subject, subjectValue, nodePropHash);
			}
		}
	}

	/**
	 * Create and store relationship property URIs at the SEMOSS base and instance levels 
	 * @throws HeaderClassException 
	 * @throws EngineException 
	 */
	public void processRelationPropURIs(XSSFSheet sheet) throws IOException
	{
		if(rdfMap.get(sheet.getSheetName().replaceAll(" ", "_") + "_RELATION_PROP") != null)
		{
			String propNames = rdfMap.get(sheet.getSheetName().replaceAll(" ", "_") + "_RELATION_PROP");
			StringTokenizer propTokens = new StringTokenizer(propNames, ";");
			relPropArrayList = new ArrayList<String>();
			if(basePropURI.equals("")){
				basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
			}
			while(propTokens.hasMoreElements())
			{

				String relation = propTokens.nextToken();
				//just in case the end of the prop string is empty string or spaces
				if(!relation.contains("%"))
					break;

				relPropArrayList.add(relation);
				logger.info("Loading relation prop " + relation);            	
				String[] strSplit = relation.split("%");
				// get the subject (index 0) and all objects for triple
				// loop through all properties on the relationship
				for(int i = 1; i < strSplit.length; i++)
				{
					String prop = strSplit[i];
					boolean headException = true;
					if(prop.contains("+"))
					{
						headException = isProperConcatHeader(prop);
					}
					else
					{
						if(!headerList.contains(prop))
							headException = false;
					}
					if(headException == false) {
						throw new IOException(prop + " cannot be found as a header");
					}
					String propURI = "";
					String property = "";
					// see if property node URI exists in prop file
					if(rdfMap.containsKey(prop))
					{
						String userProp = rdfMap.get(prop).toString(); 
						property = userProp.substring(userProp.lastIndexOf("/")+1);
					}
					// if no user specified URI, use generic URI
					else
					{
						if(prop.contains("+"))
						{
							property = processAutoConcat(prop);
						}
						else
						{
							property = prop;
						}
					}

					propURI = basePropURI+"/" + property;
					engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propURI, RDF.TYPE, basePropURI, true});
					//					createStatement(vf.createURI(propURI),RDF.TYPE,vf.createURI( basePropURI));
					//basePropURIHash.put(propURI,  propURI);
					//basePropRelations.put(propURI,  parentURI); // would need this if we were doing edge properties... but we are not any longer
				}
			}
		}
	}

	/**
	 * Change the name of nodes that are concatenations of multiple Excel columns
	 * Example: changes the string "Cat+Dog" into "CatDog"
	 * @param 	input String name of the node that is a concatenation
	 * @return 	String name of the node removing the "+" to indicate a concatenation
	 */
	public String processAutoConcat(String input)
	{
		String[] split = input.split("\\+");
		String output = "";
		for (int i=0;i<split.length;i++)
		{
			output = output+split[i];
		}
		return Utility.cleanString(output, true);
	}

	/**
	 * Determine if the node is a concatenation of multiple columns in the Excel file
	 * @param input 	String containing the name of the node
	 * @return boolean	Boolean that is true when the node is a concatenation 
	 */
	public boolean isProperConcatHeader(String input)
	{
		boolean ret = true;
		String[] split = input.split("\\+");
		for (int i=0;i<split.length;i++)
		{
			if (!headerList.contains(split[i]))
			{
				ret = false;
				break;
			}
		}
		return ret;
	}

	/**
	 * Constructs the node instance name
	 * @param subject 				String containing the node type name
	 * @param currRow, headers 		Map containing the data in the Excel file
	 * @return retString 			String containing the instance level name
	 */
	public String createInstanceValue(String subject, XSSFRow values, String[] headers)
	{
		String retString ="";
		// if node is a concatenation
		if(subject.contains("+")) 
		{
			String elements[] = subject.split("\\+");
			for (int i = 0; i<elements.length; i++)
			{
				int cell = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, subject);
				if(cell != -1 && values.getCell(cell) != null )
				{
					String value = values.getCell(cell) + "";
					value = Utility.cleanString(value, true);

					retString = retString  + value + "-";
				}
				else
				{
					retString = retString  + "null-";
				}
			}
			// a - will show up at the end of this and we need to get rid of that
			if(!retString.equals(""))
				retString = retString.substring(0,retString.length()-1);
		}
		else
		{
			int cell = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, subject);
			if(cell != -1 && values.getCell(cell) != null )
			{
				String value = values.getCell(cell) + "";
				value = Utility.cleanString(value, true);
				retString = value;
			} 
		}
		return retString;
	}

	/**
	 * Retrieves the data in the Excel file for a specified string
	 * @param object 	String containing the object to retrieve from the Excel data
	 * @param jcrMap 	Map containing the data in the Excel file
	 * @return Object	The Excel data mapped to the object string
	 */
	public Object createObject(String object, XSSFRow values, String[] headers)
	{
		// need to do the class vs. object magic
		if(object.contains("+"))
		{
			StringBuilder strBuilder = new StringBuilder();
			String[] objList = object.split("\\+");
			for(int i = 0; i < objList.length; i++){
				int cellLocation = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, objList[i]);
				if(cellLocation != -1){
					XSSFCell cell = values.getCell(cellLocation);
					if(cell != null) {
						strBuilder.append(cell.toString()); 
					}
				}
			}
			return Utility.cleanString(strBuilder.toString(), true);
		}

		int cellLocation = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, object);
		if(cellLocation != -1){
			XSSFCell cell = values.getCell(cellLocation);
			if(cell != null) {
				int cellType = cell.getCellType();
				if(cellType == XSSFCell.CELL_TYPE_BLANK) {
					//TODO: look at what happens here during testing
					return "";
				} else if (cellType == XSSFCell.CELL_TYPE_NUMERIC){
					return cell.getNumericCellValue();		
				} else if ( DateUtil.isCellDateFormatted(cell) ){
					return cell.getDateCellValue();		
				} else {
					return Utility.cleanString(cell.getStringCellValue(), true);
				}
			}
		}

		//TODO: look at what happens here during testing
		return "";
	}

	/**
	 * Setter to store the metamodel created by user as a Hashtable
	 * @param data	Hashtable<String, String> containing all the information in a properties file
	 */
	public void setRdfMapArr(Hashtable<String, String>[] rdfMapArr) {
		this.rdfMapArr = rdfMapArr;
		propFileExist = false;
	}

	/**
	 * Load the Excel workbook
	 * @param fileName String
	 * @throws IOException 
	 */
	public void openExcelWorkbook(String fileName) throws IOException {
		FileInputStream poiReader = null;
		try {
			poiReader = new FileInputStream(fileName);
			workbook = new XSSFWorkbook(poiReader);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Could not read Excel file located at " + fileName);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("File: " + fileName + " is not a valid Microsoft Excel (.xlsx, .xlsm) file");
		}
	}

	/**
	 * Load the Excel workbook
	 * @param fileName String
	 * @throws IOException 
	 */
	public void closeExcelWorkbook() throws IOException {
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