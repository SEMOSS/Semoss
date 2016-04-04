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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Loading data into SEMOSS using comma separated value (CSV) files
 */
public class CSVReader extends AbstractFileReader {

	private static final Logger logger = LogManager.getLogger(CSVReader.class.getName());
	
	private String propFile; // the file that serves as the property file
	private FileReader readCSVFile;
	private ICsvMapReader mapReader;
	private String [] header;
	private List<String> headerList;
	private CellProcessor[] processors;
	private static Hashtable <String, CellProcessor> typeHash = new Hashtable<String, CellProcessor>();
	public final static String NUMCOL = "NUM_COLUMNS";
	public final static String NOT_OPTIONAL = "NOT_OPTIONAL";
	private ArrayList<String> relationArrayList = new ArrayList<String>();
	private ArrayList<String> nodePropArrayList = new ArrayList<String>();
	private ArrayList<String> relPropArrayList = new ArrayList<String>();
	private boolean propFileExist = true;
	private Hashtable<String, String>[] rdfMapArr;
	
	private int count = 0;
	private int startRow = 2;
	private int maxRows = 100000;

	/**
	 * Loading data into SEMOSS to create a new database
	 * @param dbName 		String grabbed from the user interface that would be used as the name for the database
	 * @param fileNames		Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase	String grabbed from the user interface that is used as the URI base for all instances 
	 * @param customMap		
	 * @param owlFile		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 * @return 
	 * @throws EngineException 
	 * @throws FileReaderException 
	 * @throws FileWriterException 
	 * @throws HeaderClassException 
	 */
	public IEngine importFileWithOutConnection(String smssLocation, String engineName, String fileNames, String customBase, String owlFile) 
			throws FileNotFoundException, IOException {
		boolean error = false;
		logger.setLevel(Level.WARN);
		String[] files = prepareReader(fileNames, customBase, owlFile, smssLocation);
		try {
			openRdfEngineWithoutConnection(engineName);
			createTypes();
			for(int i = 0; i<files.length;i++)
			{
				try {
					String fileName = files[i];
					openCSVFile(fileName);			
					// load the prop file for the CSV file 
					if(propFileExist){
						openProp(propFile);
					} else {
						rdfMap = rdfMapArr[i];
					}
					//TODO: same method used in other classes
					parseMetadata();
					// determine the type of data in each column of CSV file
					createProcessors();
//					processConceptRelationURIs();
//					processNodePropURIs();
//					processRelationPropURIs();
					processDisplayNames();
					skipRows();
					processRelationShips();
				} finally {
					closeCSVFile();
				}
			}
			loadMetadataIntoEngine();
			createBaseRelations();
		} catch(FileNotFoundException e) {
			error = true;
			throw new FileNotFoundException(e.getMessage());
		} catch(IOException e) {
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
	 * Load data into SEMOSS into an existing database
	 * @param engineName 	String grabbed from the user interface specifying which database to add the data
	 * @param fileNames 	Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase 	String grabbed from the user interface that is used as the URI base for all instances
	 * @param customMap 	Absolute path specified in the CSV file that determines the location of the prop file for the data
	 * @param owlFile 		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 * @throws FileNotFoundException 
	 * @throws IOException 
	 */
	public void importFileWithConnection(String engineName, String fileNames, String customBase, String owlFile) throws FileNotFoundException, IOException {
		logger.setLevel(Level.WARN);
		String[] files = prepareReader(fileNames, customBase, owlFile, engineName);
		openEngineWithConnection(engineName);
		createTypes();
		for(int i = 0; i<files.length;i++)
		{
			String fileName = files[i];
			openCSVFile(fileName);	
			try {
				// load the prop file for the CSV file 
				if(propFileExist){
					openProp(propFile);
				} else {
					rdfMap = rdfMapArr[i];
				}
				// determine the type of data in each column of CSV file
				createProcessors();
				parseMetadata();
//				processConceptRelationURIs();
//				processNodePropURIs();
//				processRelationPropURIs();
				processDisplayNames();
				skipRows();
				processRelationShips();
			} finally {
				closeCSVFile();
			}
		} 
		loadMetadataIntoEngine();
		createBaseRelations();
		commitDB();
		engine.loadTransformedNodeNames();
	}

	/**
	 * Stores all possible variable types that the user can input from the CSV file into hashtable
	 * Hashtable is then used to match each column in CSV to a specific type based on user input in prop file
	 */
	public static void createTypes()
	{
		typeHash.put("DECIMAL", new ParseDouble());
		typeHash.put("DOUBLE", new ParseDouble());
		typeHash.put("STRING", new NotNull());
		typeHash.put("TEXT", new NotNull());
		typeHash.put("DATE", new ParseDate("yyyy-MM-dd hh:mm:ss"));
		typeHash.put("SIMPLEDATE", new ParseDate("MM/dd/yyyy"));
		// currently only add in numbers as doubles
		typeHash.put("NUMBER", new ParseDouble());
		typeHash.put("INTEGER", new ParseDouble());
		//		typeHash.put("NUMBER", new ParseInt());
		//		typeHash.put("INTEGER", new ParseInt());
		typeHash.put("BOOLEAN", new ParseBool());

		// now the optionals
		typeHash.put("DECIMAL_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("DOUBLE_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("STRING_OPTIONAL", new Optional());
		typeHash.put("TEXT_OPTIONAL", new Optional());
		typeHash.put("DATE_OPTIONAL", new Optional(new ParseDate("yyyy-MM-dd HH:mm:ss")));
		typeHash.put("SIMPLEDATE_OPTIONAL", new Optional(new ParseDate("MM/dd/yyyy")));
		// currently only add in numbers as doubles
		typeHash.put("NUMBER_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("INTEGER_OPTIONAL", new Optional(new ParseDouble()));
		//		typeHash.put("NUMBER_OPTIONAL", new Optional(new ParseInt()));
		//		typeHash.put("INTEGER_OPTIONAL", new Optional(new ParseInt()));
		typeHash.put("BOOLEAN_OPTIONAL", new Optional(new ParseBool()));
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
	 * Matches user inputed column type in prop file to the specific variable type name within Java SuperCSV API
	 */
	public void createProcessors()
	{
		// get the number columns in CSV file
		int numColumns = Integer.parseInt(rdfMap.get(NUMCOL));
		// Columns in prop file that are NON_OPTIMAL must contain a value
		String optional = ";";
		if(rdfMap.get(NOT_OPTIONAL) != null)
		{
			optional  = rdfMap.get(NOT_OPTIONAL);
		}

		int offset = 0;
		if(propFileExist){
			offset = 1;
		}
		processors = new CellProcessor[numColumns+offset];
		for(int procIndex = 1;procIndex <= processors.length;procIndex++)
		{
			// find the type for each column
			String type = rdfMap.get(procIndex+"");
			boolean opt = true;
			if(optional.indexOf(";" + procIndex + ";") > 1)
				opt = false;

			if(type != null && opt)
				processors[procIndex-1] = typeHash.get(type.toUpperCase() + "_OPTIONAL");
			else if(type != null)
				processors[procIndex-1] = typeHash.get(type.toUpperCase());
			else if(type == null)
				processors[procIndex-1] = typeHash.get("STRING_OPTIONAL");
		}
	}

	/**
	 * Specifies which rows in the CSV to load based on user input in the prop file
	 * @throws FileReaderException 
	 */
	public void skipRows() throws IOException {
		//start count at 1 just row 1 is the header
		count = 1;
		if (rdfMap.get("START_ROW") != null)
			startRow = Integer.parseInt(rdfMap.get("START_ROW")); 
		try {
			while( count<startRow-1 && mapReader.read(header, processors) != null)// && count<maxRows)
			{
				count++;
				logger.info("Skipping line: " + count);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Error processing CSV headers");
		}
	}

	/**
	 * Create all the triples associated with the relationships specified in the prop file
	 * @throws IOException 
	 */
	public void processRelationShips() throws IOException 
	{
		// get all the relation
		Map<String, Object> jcrMap;
		// overwrite this value if user specified the max rows to load
		if (rdfMap.get("END_ROW") != null)
		{
			maxRows =  Integer.parseInt(rdfMap.get("END_ROW"));
		}
		// only start from the maxRow - the startRow
		// added -1 is because of index nature
		// the earlier rows should already have been skipped
		try {
			while( (jcrMap = mapReader.read(header, processors)) != null && count<(maxRows))
			{
				count++;
				logger.info("Process line: " +count);

				// process all relationships in row
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

					String subjectValue = createInstanceValue(sub, jcrMap);
					String objectValue = createInstanceValue(obj, jcrMap);
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
							for(int i = 1; i < relPropSplit.length; i++)
							{			
								// add the necessary triples for the relationship property
								String prop = relPropSplit[i];
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
								propHash.put(property, createObject(prop, jcrMap));
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
					String subjectValue = createInstanceValue(sub, jcrMap);
					// loop through all properties on the node
					for(int i = 1; i < strSplit.length; i++)
					{
						String prop = strSplit[i];
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
						String propValue = createLiteralValue(prop, jcrMap);
						if (subjectValue.isEmpty() || propValue.isEmpty())
						{
							continue;
						}
						nodePropHash.put(property, createObject(prop, jcrMap));
					}
					addNodeProperties(subject, subjectValue, nodePropHash);
				}
			}
		} catch(SuperCsvCellProcessorException e) {
			e.printStackTrace();
			String errorMessage = "Error processing row number " + count + ". ";
			errorMessage += e.getMessage();
			throw new IOException(errorMessage);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Error processing CSV headers");
		}
		System.out.println("FINAL COUNT " + count);
	}

	/**
	 * Create and store concept and relation URIs at the SEMOSS base and instance levels
	 * @throws IOException 
	 */
//	public void processConceptRelationURIs() throws IOException {
//		// get the list of relationships from the prop file
//		if(rdfMap.get("RELATION") != null)
//		{
//			String relationNames = rdfMap.get("RELATION");
//			StringTokenizer relationTokens = new StringTokenizer(relationNames, ";");
//			relationArrayList = new ArrayList<String>();
//			// process each relationship
//			while(relationTokens.hasMoreElements())
//			{
//				String relation = relationTokens.nextToken();
//				// just in case the end of the prop string is empty string or spaces
//				if(!relation.contains("@"))
//					break;
//
//				relationArrayList.add(relation);
//				logger.info("Loading relation " + relation);            	
//				String[] strSplit = relation.split("@");
//				// get the subject and object for triple (the two indexes)
//				String sub = strSplit[0];
//				String subject = "";
//				String predicate = strSplit[1];
//				String obj = strSplit[2];
//				String object = "";
//				// check if prop file entries are not in excel and if nodes are concatenations
//				// throw exception if prop file entries not in excel
//				boolean headException = true;
//				if(sub.contains("+"))
//				{
//					headException = isProperConcatHeader(sub);
//				}
//				else
//				{
//					if(!headerList.contains(sub))
//						headException = false;
//				}
//				if(headException == false) {
//					throw new IOException(sub + " cannot be found as a header");
//				}
//				if(obj.contains("+"))
//				{
//					headException = isProperConcatHeader(obj);
//				}
//				else
//				{
//					if(!headerList.contains(obj))
//						headException = false;
//				}
//				if(headException == false) {
//					throw new IOException(obj + " cannot be found as a header");
//				}
//				// create concept uris
//				String relURI = "";
//				String relBaseURI = "";
//				String idxBaseURI = "";
//				String idxURI = "";
//
//				// see if subject node SEMOSS base URI exist in prop file first
//				if(rdfMap.containsKey(sub+Constants.CLASS))
//				{
//					baseConceptURIHash.put(sub,rdfMap.get(sub+Constants.CLASS));
//				}
//				// if no user specific URI, use generic SEMOSS base URI
//				else
//				{
//					if(sub.contains("+"))
//					{
//						subject = processAutoConcat(sub);
//						idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;
//					}
//					else
//					{
//						subject = sub;
//						idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;
//					}
//					baseConceptURIHash.put(subject, idxBaseURI);
//				}
//				// see if subject node instance URI exists in prop file
//				if(rdfMap.containsKey(sub))
//				{
//					String userSub = rdfMap.get(sub).toString(); 
//					subject = userSub.substring(userSub.lastIndexOf("/"));
//					conceptURIHash.put(sub, userSub);
//				}
//				// if no user specified URI, use generic custombaseURI
//				else
//				{
//					if(sub.contains("+"))
//					{
//						subject = processAutoConcat(sub);
//						idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;
//					}
//					else
//					{
//						subject = sub;
//						idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;
//					}
//					conceptURIHash.put(subject, idxURI);
//				}
//				// see if object node SEMOSS base URI exists in prop file
//				if(rdfMap.containsKey(obj+Constants.CLASS))
//				{
//					baseConceptURIHash.put(obj,rdfMap.get(obj+Constants.CLASS));
//				}
//				// if no user specified URI, use generic SEMOSS base URI
//				else
//				{
//					if(obj.contains("+"))
//					{
//						object = processAutoConcat(obj);
//						idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ object;
//					}
//					else
//					{
//						object = obj;
//						idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ object;
//					}
//					baseConceptURIHash.put(object, idxBaseURI);
//				}
//				// see if object node instance URI exists in prop file
//				if(rdfMap.containsKey(obj))
//				{
//					String userObj = rdfMap.get(obj).toString(); 
//					object = userObj.substring(userObj.lastIndexOf("/")+1);
//					conceptURIHash.put(obj, userObj);
//				}
//				// if no user specified URI, use generic custombaseURI
//				else
//				{
//					if(obj.contains("+"))
//					{
//						object = processAutoConcat(obj);
//						idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ object;
//					}
//					else
//					{
//						object = obj;
//						idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ object;
//					}
//					conceptURIHash.put(object, idxURI);
//				}
//				// add relation uri into basehash and urihash
//				String relPropString = subject + "_"+ predicate + "_" + object; //this string concat shows up in prop file
//
//				// see if relationship SEMOSS base URI exists in prop file
//				if(rdfMap.containsKey(relPropString+Constants.CLASS)) {
//					baseRelationURIHash.put(relPropString+Constants.CLASS,rdfMap.get(relPropString+Constants.CLASS));
//				}
//				// if no user specified URI, use generic SEMOSS base URI
//				else
//				{
//					relBaseURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + predicate;
//					baseRelationURIHash.put(relPropString+Constants.CLASS, relBaseURI);
//				}
//				// see if relationship URI exists in prop file
//				if(rdfMap.containsKey(relPropString)) {
//					relationURIHash.put(relPropString,rdfMap.get(relPropString));
//				}
//				// if no user specified URI, use generic custombaseURI
//				else {
//					relURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + predicate;
//					relationURIHash.put(relPropString, relURI);
//				}
//			}
//		}
//	}

	/**
	 * Create and store node property URIs at the SEMOSS base and instance levels 
	 * @throws IOException 
	 */
//	public void processNodePropURIs() throws IOException 
//	{
//		if(rdfMap.get("NODE_PROP") != null)
//		{
//			String nodePropNames = rdfMap.get("NODE_PROP");
//			StringTokenizer nodePropTokens = new StringTokenizer(nodePropNames, ";");
//			nodePropArrayList = new ArrayList<String>();
//			if(basePropURI.equals("")){
//				basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
//			}
//			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{basePropURI, Constants.SUBPROPERTY_URI, basePropURI, true});
////			createStatement(vf.createURI(basePropURI),vf.createURI(),vf.createURI(basePropURI));
//
//			while(nodePropTokens.hasMoreElements())
//			{
//				String relation = nodePropTokens.nextToken();
//				// in case the end of the prop string is empty string or spaces
//				if(!relation.contains("%"))
//					break;
//
//				nodePropArrayList.add(relation);
//				logger.info("Loading Node Prop " + relation);            	
//				String[] strSplit = relation.split("%");
//				// get the subject and object for triple (the two indexes)
//				String sub = strSplit[0];
//				String subject = "";
//				// loop through all properties on the node
//				for(int i = 1; i < strSplit.length; i++)
//				{
//					String prop = strSplit[i];
//					String idxBaseURI = "";
//					String idxURI = "";
//					String propURI = "";
//
//					boolean headException = true;
//					if(sub.contains("+"))
//					{
//						headException = isProperConcatHeader(sub);
//					}
//					else
//					{
//						if(!headerList.contains(sub))
//							headException = false;
//					}
//					if(headException == false) {
//						throw new IOException(sub + " cannot be found as a header");
//					}
//					if(prop.contains("+"))
//					{
//						headException = isProperConcatHeader(prop);
//					}
//					else
//					{
//						if(!headerList.contains(prop))
//							headException = false;
//					}
//					if(headException == false) {
//						throw new IOException(prop + " cannot be found as a header");
//					}
//					// see if subject node SEMOSS base URI exists in prop file
//					if(rdfMap.containsKey(sub+Constants.CLASS))
//					{
//						idxBaseURI = rdfMap.get(sub+Constants.CLASS);
//						baseConceptURIHash.put(sub,idxBaseURI);
//					}
//					// if no user specified URI, use generic SEMOSS base URI
//					else
//					{
//						if(sub.contains("+"))
//						{
//							subject = processAutoConcat(sub);
//							idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;						
//						}
//						else
//						{
//							subject = sub;
//							idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;						
//						}
//						baseConceptURIHash.put(subject, idxBaseURI);
//					}
//					// see if subject node instance URI exists in prop file
//					if(rdfMap.containsKey(sub))
//					{
//						subject = rdfMap.get(sub);
//						conceptURIHash.put(sub, rdfMap.get(sub));
//					}
//					// if no user specified URI, use generic custombaseURI
//					else
//					{
//						if(sub.contains("+"))
//						{
//							subject = processAutoConcat(sub);
//							idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;						
//						}
//						else
//						{
//							subject = sub;
//							idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;
//
//						}
//						conceptURIHash.put(subject, idxURI);	
//					}
//
//					String property = "";
//					// see if property node URI exists in prop file
//					if(rdfMap.containsKey(prop))
//					{
//						String userProp = rdfMap.get(prop).toString(); 
//						property = userProp.substring(userProp.lastIndexOf("/")+1);
//					}
//					// if no user specified URI, use generic URI
//					else
//					{
//						if(prop.contains("+"))
//						{
//							property = processAutoConcat(prop);
//						}
//						else
//						{
//							property = prop;
//						}
//					}
//
//					propURI = basePropURI+"/" + property;
//					engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propURI, RDF.TYPE, Utility.cleanString(basePropURI, false), true});
//					// basePropURIHash.put(propURI,  propURI);
//					// basePropRelations.put(propURI,  idxBaseURI);
//				}
//			}
//		}
//	}

	/**
	 * Create and store relationship property URIs at the SEMOSS base and instance levels 
	 * @throws IOException 
	 */
//	public void processRelationPropURIs() throws IOException
//	{
//		if(rdfMap.get("RELATION_PROP") != null)
//		{
//			String propNames = rdfMap.get("RELATION_PROP");
//			StringTokenizer propTokens = new StringTokenizer(propNames, ";");
//			relPropArrayList = new ArrayList<String>();
//			if(basePropURI.equals("")){
//				basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
//			}
//			while(propTokens.hasMoreElements())
//			{
//
//				String relation = propTokens.nextToken();
//				//just in case the end of the prop string is empty string or spaces
//				if(!relation.contains("%"))
//					break;
//
//				relPropArrayList.add(relation);
//				logger.info("Loading relation prop " + relation);            	
//				String[] strSplit = relation.split("%");
//				// get the subject (index 0) and all objects for triple
//				// loop through all properties on the relationship
//				for(int i = 1; i < strSplit.length; i++)
//				{
//					String prop = strSplit[i];
//					boolean headException = true;
//					if(prop.contains("+"))
//					{
//						headException = isProperConcatHeader(prop);
//					}
//					else
//					{
//						if(!headerList.contains(prop))
//							headException = false;
//					}
//					if(headException == false) {
//						throw new IOException(prop + " cannot be found as a header");
//					}
//					String propURI = "";
//					String property = "";
//					// see if property node URI exists in prop file
//					if(rdfMap.containsKey(prop))
//					{
//						String userProp = rdfMap.get(prop).toString(); 
//						property = userProp.substring(userProp.lastIndexOf("/")+1);
//					}
//					// if no user specified URI, use generic URI
//					else
//					{
//						if(prop.contains("+"))
//						{
//							property = processAutoConcat(prop);
//						}
//						else
//						{
//							property = prop;
//						}
//					}
//
//					propURI = basePropURI+"/" + property;
//					engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propURI, RDF.TYPE,  Utility.cleanString(basePropURI, false), true});
//					//basePropURIHash.put(propURI,  propURI);
//					//basePropRelations.put(propURI,  parentURI); // would need this if we were doing edge properties... but we are not any longer
//				}
//			}
//		}
//	}

	/**
	 * Change the name of nodes that are concatenations of multiple CSV columns
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
	 * Determine if the node is a concatenation of multiple columns in the CSV file
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
	 * @param subject 		String containing the node type name
	 * @param jcrMap 		Map containing the data in the CSV file
	 * @return retString 	String containing the instance level name
	 */
	public String createInstanceValue(String subject, Map <String, Object> jcrMap)
	{
		String retString ="";
		// if node is a concatenation
		if(subject.contains("+")) 
		{
			String elements[] = subject.split("\\+");
			for (int i = 0; i<elements.length; i++)
			{
				String subjectElement = elements[i];
				if(jcrMap.containsKey(subjectElement) && jcrMap.get(subjectElement)!= null)
				{
					String value = jcrMap.get(subjectElement) + "";
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
			if(jcrMap.containsKey(subject) && jcrMap.get(subject)!= null)
			{
				String value = jcrMap.get(subject) + "";
				value = Utility.cleanString(value, true);
				retString = value;
			}
		}
		return retString;
	}
	
	/**
	 * Constructs the property literal
	 * @param subject 		String containing the node type name
	 * @param jcrMap 		Map containing the data in the CSV file
	 * @return retString 	String containing the instance level name
	 */
	public String createLiteralValue(String subject, Map <String, Object> jcrMap)
	{
		String retString ="";
		// if node is a concatenation
		if(subject.contains("+")) 
		{
			String elements[] = subject.split("\\+");
			for (int i = 0; i<elements.length; i++)
			{
				String subjectElement = elements[i];
				if(jcrMap.containsKey(subjectElement) && jcrMap.get(subjectElement)!= null)
				{
					String value = jcrMap.get(subjectElement) + "";
					value = Utility.cleanString(value, true, false, true);

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
			if(jcrMap.containsKey(subject) && jcrMap.get(subject)!= null)
			{
				String value = jcrMap.get(subject) + "";
				value = Utility.cleanString(value, true, false, true);
				retString = value;
			}
		}
		return retString;
	}

	/**
	 * Retrieves the data in the CSV file for a specified string
	 * @param object 	String containing the object to retrieve from the CSV data
	 * @param jcrMap 	Map containing the data in the CSV file
	 * @return Object	The CSV data mapped to the object string
	 */
	public Object createObject(String object, Map <String, Object> jcrMap)
	{
		// need to do the class vs. object magic
		if(object.contains("+"))
		{
			StringBuilder strBuilder = new StringBuilder();
			String[] objList = object.split("\\+");
			for(int i = 0; i < objList.length; i++){
				strBuilder.append(jcrMap.get(objList[i])); 
			}
			return Utility.cleanString(strBuilder.toString(), true);
		}

		return jcrMap.get(object);
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
	 * Load the CSV file
	 * Gets the headers for each column and reads the property file
	 * @param fileName String
	 * @throws FileNotFoundException 
	 */
	public void openCSVFile(String fileName) throws FileNotFoundException {
		try {
			readCSVFile = new FileReader(fileName);
			mapReader = new CsvMapReader(readCSVFile, CsvPreference.STANDARD_PREFERENCE);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new FileNotFoundException("Could not find CSV file located at " + fileName);
		}		
		try {
			header = mapReader.getHeader(true);
			//header clean up, consistent with RDBMS headers handling
			/*for(int j = 0; j < header.length; j++){
				String singleHeader = Utility.cleanVariableString(header[j]); //String singleHeader = header[j];
				header[j] = singleHeader;
			}*/
			headerList = Arrays.asList(header);
			// last header in CSV file is the absolute path to the prop file
			propFile = header[header.length-1];
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileNotFoundException("Could not close reader input stream for CSV file " + fileName);
		}
	}
	
	/**
	 * Closes the CSV file streams
	 * @throws IOException 
	 */
	public void closeCSVFile() throws IOException {
		try {
			readCSVFile.close();
			mapReader.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Could not close CSV file streams");
		}		
	}


}