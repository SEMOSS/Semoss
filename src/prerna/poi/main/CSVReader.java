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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
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
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

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
 * Loading data into SEMOSS using comma separated value (CSV) files
 */
public class CSVReader extends AbstractFileReader {

	ICsvMapReader mapReader;
	String [] header;
	List<String> headerList;
	CellProcessor[] processors;
	static Hashtable <String, CellProcessor> typeHash = new Hashtable<String, CellProcessor>();
	public static String NUMCOL = "NUM_COLUMNS";
	public static String NOT_OPTIONAL = "NOT_OPTIONAL";
	ArrayList<String> relationArrayList, nodePropArrayList, relPropArrayList;
	int count = 0;
	
	boolean propFileExist = true;
	
	/**
	 * The main method is never called within SEMOSS
	 * Used to load data without having to start SEMOSS
	 * User must specify location of all files manually inside the method
	 * @param args String[]
	 */
	public static void main(String[] args) throws Exception
	{
		CSVReader reader = new CSVReader();
		String workingDir = System.getProperty("user.dir");
		reader.customBaseURI = "http://health.mil/ontologies";
		reader.semossURI = "http://semoss.org/ontologies";
		reader.createTypes();
		String bdPropFile = workingDir + "/db/DOJ2 - Copy.smss";

//		reader.loadBDProperties(bdPropFile);
//		reader.openDB();

		ArrayList<String> files = new ArrayList<String>();
		files.add(workingDir+"/JEAD_Systems_SEMOSS_Test2.csv");
		for(int i = 0; i<files.size();i++)
		{
			String fileName = files.get(i);
			reader.openCSVFile(fileName);
			// load the prop file for the CSV file 
			reader.openProp(propFile);
			// load the big data properties file
			// create processors based on property file
			reader.createProcessors();
			// DB
			// Process
			reader.processConceptRelationURIs();
			reader.processNodePropURIs();
			reader.processRelationPropURIs();
			reader.skipRows();
			reader.processRelationShips();
		}
//		reader.openOWLWithOutConnection();
		reader.createBaseRelations();
		reader.closeDB();
	}

	/**
	 * Loading data into SEMOSS to create a new database
	 * @param dbName 		String grabbed from the user interface that would be used as the name for the database
	 * @param fileNames		Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase	String grabbed from the user interface that is used as the URI base for all instances 
	 * @param customMap		
	 * @param owlFile		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 */
	public void importFileWithOutConnection(String engineName, String fileNames, String customBase, String owlFile) throws Exception  
	{
		String[] files = prepareReader(fileNames, customBase, owlFile);
		openEngineWithoutConnection(engineName);
		
		for(int i = 0; i<files.length;i++)
		{
			String fileName = files[i];
			openCSVFile(fileName);			
			// load the prop file for the CSV file 
			if(propFileExist){
				openProp(propFile);
			}
			// determine the type of data in each column of CSV file
			createProcessors();
			processConceptRelationURIs();
			processNodePropURIs();
			processRelationPropURIs();
			skipRows();
			processRelationShips();
		}
		createBaseRelations();
		closeDB();
	}

	/**
	 * Load data into SEMOSS into an existing database
	 * @param engineName 	String grabbed from the user interface specifying which database to add the data
	 * @param fileNames 	Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase 	String grabbed from the user interface that is used as the URI base for all instances
	 * @param customMap 	Absolute path specified in the CSV file that determines the location of the prop file for the data
	 * @param owlFile 		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 */
	public void importFileWithConnection(String engineName, String fileNames, String customBase, String owlFile) throws Exception 
	{
		String[] files = prepareReader(fileNames, customBase, owlFile);
		openEngineWithConnection(engineName);
		
		createTypes();
		for(int i = 0; i<files.length;i++)
		{
			String fileName = files[i];
			openCSVFile(fileName);			
			// load the prop file for the CSV file 
			if(propFileExist){
				openProp(propFile);
			}
			// determine the type of data in each column of CSV file
			createProcessors();
			processConceptRelationURIs();
			processNodePropURIs();
			processRelationPropURIs();
			skipRows();
			processRelationShips();
		}
		createBaseRelations();
		commitDB();
	}

	/**
	 * Stores all possible variable types that the user can input from the CSV file into hashtable
	 * Hashtable is then used to match each column in CSV to a specific type based on user input in prop file
	 */
	public static void createTypes()
	{
		typeHash.put("DECIMAL", new ParseDouble());
		typeHash.put("STRING", new NotNull());
		typeHash.put("DATE", new ParseDate("yyyy-mm-dd hh:mm:ss"));
		typeHash.put("SIMPLEDATE", new ParseDate("mm/dd/yyyy"));
		typeHash.put("NUMBER", new ParseInt());
		typeHash.put("BOOLEAN", new ParseBool());

		// now the optionals
		typeHash.put("DECIMAL_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("STRING_OPTIONAL", new Optional());
		typeHash.put("DATE_OPTIONAL", new Optional(new ParseDate("yyyy-MM-dd HH:mm:ss")));
		typeHash.put("SIMPLEDATE_OPTIONAL", new Optional(new ParseDate("mm/dd/yyyy")));
		typeHash.put("NUMBER_OPTIONAL", new Optional(new ParseInt()));
		typeHash.put("BOOLEAN_OPTIONAL", new Optional(new ParseBool()));
	}

	/**
	 * Matches user inputed column type in prop file to the specific variable type name within Java SuperCSV API
	 */
	public void createProcessors()
	{
		// get the number columns in CSV file
		int numColumns = Integer.parseInt(rdfMap.get(NUMCOL));
		// Columns in prop file that are NON_OPTIMAL must contain a value
		String optional  = rdfMap.get(NOT_OPTIONAL);
		
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
	 */
	public void skipRows() throws Exception
	{
		Map<String, Object> jcrMap;
		//start count at 1 just row 1 is the header
		count = 1;
		int startRow = 2;
		if (rdfMap.get("START_ROW") != null)
			startRow = Integer.parseInt(rdfMap.get("START_ROW")); 
		while( count<startRow-1 && mapReader.read(header, processors) != null)// && count<maxRows)
		{
			count++;
			logger.info("Skipping line: " + count);
		}
	}

	/**
	 * Create all the triples associated with the relationships specified in the prop file
	 */
	public void processRelationShips() throws Exception
	{
		// get all the relation
		Map<String, Object> jcrMap;
		// max row predetermined value
		int maxRows = 10000;
		// overwrite this value if user specified the max rows to load
		if (rdfMap.get("END_ROW") != null)
			maxRows =  Integer.parseInt(rdfMap.get("END_ROW"));
		// only start from the maxRow - the startRow
		// added -1 is because of index nature
		// the earlier rows should already have been skipped
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
				String subject = strSplit[0];
				String predicate = strSplit[1];
				String object = strSplit[2];

				String subjectValue = createInstanceValue(subject, jcrMap);
				String objectValue = createInstanceValue(object, jcrMap);
				if (subjectValue.equals("") || objectValue.equals(""))
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
//							createProperty(predicateInstanceURI, propURI, relPropSplit[i], jcrMap);
							propHash.put(prop, createObject(prop, jcrMap));
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
				String subject = strSplit[0];
				String subjectValue = createInstanceValue(subject, jcrMap);
				// loop through all properties on the node
				for(int i = 1; i < strSplit.length; i++)
				{
					String prop = strSplit[i];
					nodePropHash.put(prop, createObject(prop, jcrMap));
				}
				addNodeProperties(subject, subjectValue, nodePropHash);
			}
		}
	}

	/**
	 * Create and store concept and relation URIs at the SEMOSS base and instance levels
	 */
	public void processConceptRelationURIs() throws Exception{
		// get the list of relationships from the prop file
		String relationNames = rdfMap.get("RELATION");
		StringTokenizer relationTokens = new StringTokenizer(relationNames, ";");
		relationArrayList = new ArrayList<String>();
		// process each relationship
		while(relationTokens.hasMoreElements())
		{
			String relation = relationTokens.nextToken();
			// just in case the end of the prop string is empty string or spaces
			if(!relation.contains("@"))
				break;

			relationArrayList.add(relation);
			logger.info("Loading relation " + relation);            	
			String[] strSplit = relation.split("@");
			// get the subject and object for triple (the two indexes)
			String subject = strSplit[0];
			String predicate = strSplit[1];
			String object = strSplit[2];
			// check if prop file entries are not in excel and if nodes are concatenations
			// throw exception if prop file entries not in excel
			boolean headException = true;
			if(subject.contains("+"))
			{
				headException = isProperConcatHeader(subject);
			}
			else
			{
				if(!headerList.contains(subject))
					headException = false;
			}
			if(headException = false)
				throw new Exception();

			if(object.contains("+"))
			{
				headException = isProperConcatHeader(object);
			}
			else
			{
				if(!headerList.contains(object))
					headException = false;
			}
			if(headException = false)
				throw new Exception();

			// create concept uris
			String relURI = "";
			String relBaseURI = "";
			String idxBaseURI = "";
			String idxURI = "";

			// see if subject node SEMOSS base URI exist in prop file first
			if(rdfMap.containsKey(subject+Constants.CLASS))
			{
				baseConceptURIHash.put(subject+Constants.CLASS,rdfMap.get(subject+Constants.CLASS));
			}
			// if no user specific URI, use generic SEMOSS base URI
			else
			{
				if(subject.contains("+"))
				{
					String processedSubject = processAutoConcat(subject);
					idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ processedSubject;
				}
				else
				{
					idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;
				}
				baseConceptURIHash.put(subject+Constants.CLASS, idxBaseURI);
			}
			// see if subject node instance URI exists in prop file
			if(rdfMap.containsKey(subject))
			{
				conceptURIHash.put(subject, rdfMap.get(subject));
			}
			// if no user specified URI, use generic custombaseURI
			else
			{
				if(subject.contains("+"))
				{
					String processedSubject = processAutoConcat(subject);
					idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ processedSubject;
				}
				else
				{
					idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;
				}
				conceptURIHash.put(subject, idxURI);
			}
			// see if object node SEMOSS base URI exists in prop file
			if(rdfMap.containsKey(object+Constants.CLASS))
			{
				baseConceptURIHash.put(object+Constants.CLASS,rdfMap.get(object+Constants.CLASS));
			}
			// if no user specified URI, use generic SEMOSS base URI
			else
			{
				if(object.contains("+"))
				{
					String processedObject = processAutoConcat(object);
					idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ processedObject;
				}
				else
				{
					idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ object;
				}
				baseConceptURIHash.put(object+Constants.CLASS, idxBaseURI);
			}
			// see if object node instance URI exists in prop file
			if(rdfMap.containsKey(object))
			{
				conceptURIHash.put(object, rdfMap.get(object));
			}
			// if no user specified URI, use generic custombaseURI
			else
			{
				if(object.contains("+"))
				{
					String processedObject = processAutoConcat(object);
					idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ processedObject;
				}
				else
				{
					idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ object;
				}
				conceptURIHash.put(object, idxURI);
			}
			// add relation uri into basehash and urihash
			String relPropString = subject + "_"+ predicate + "_" + object; //this string concat shows up in prop file

			// see if relationship SEMOSS base URI exists in prop file
			if(rdfMap.containsKey(relPropString+Constants.CLASS)) {
				baseRelationURIHash.put(relPropString+Constants.CLASS,rdfMap.get(relPropString+Constants.CLASS));
			}
			// if no user specified URI, use generic SEMOSS base URI
			else
			{
				relBaseURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + predicate;
				baseRelationURIHash.put(relPropString+Constants.CLASS, relBaseURI);
			}
			// see if relationship URI exists in prop file
			if(rdfMap.containsKey(relPropString)) {
				relationURIHash.put(relPropString,rdfMap.get(relPropString));
			}
			// if no user specified URI, use generic custombaseURI
			else {
				relURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + predicate;
				relationURIHash.put(relPropString, relURI);
			}
		}		
	}

	/**
	 * Create and store node property URIs at the SEMOSS base and instance levels 
	 */
	public void processNodePropURIs() throws Exception
	{
		String nodePropNames = rdfMap.get("NODE_PROP");
		StringTokenizer nodePropTokens = new StringTokenizer(nodePropNames, ";");
		nodePropArrayList = new ArrayList<String>();
		if(basePropURI.equals("")){
			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		}
		createStatement(vf.createURI(basePropURI),vf.createURI(Constants.SUBPROPERTY_URI),vf.createURI(basePropURI));

		for(int relIndex = 0;nodePropTokens.hasMoreElements();relIndex++)
		{
			String relation = nodePropTokens.nextToken();
			// in case the end of the prop string is empty string or spaces
			if(!relation.contains("%"))
				break;

			nodePropArrayList.add(relation);
			logger.info("Loading Node Prop " + relation);            	
			String[] strSplit = relation.split("%");
			// get the subject and object for triple (the two indexes)
			String subject = strSplit[0];
			// loop through all properties on the node
			for(int i = 1; i < strSplit.length; i++)
			{
				String prop = strSplit[i];
				String idxBaseURI = "";
				String idxURI = "";
				String propURI = "";

				boolean headException = true;
				if(subject.contains("+"))
				{
					headException = isProperConcatHeader(subject);
				}
				else
				{
					if(!headerList.contains(subject))
						headException = false;
				}
				if(headException = false)
					throw new Exception();

				if(prop.contains("+"))
				{
					headException = isProperConcatHeader(prop);
				}
				else
				{
					if(!headerList.contains(prop))
						headException = false;
				}
				if(headException = false)
					throw new Exception();

				// see if subject node SEMOSS base URI exists in prop file
				if(rdfMap.containsKey(subject+Constants.CLASS))
				{
					baseConceptURIHash.put(subject+Constants.CLASS,rdfMap.get(subject));
				}
				// if no user specified URI, use generic SEMOSS base URI
				else
				{
					idxBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;
					baseConceptURIHash.put(subject+Constants.CLASS, idxBaseURI);
				}
				// see if subject node instance URI exists in prop file
				if(rdfMap.containsKey(subject))
				{
					conceptURIHash.put(subject, rdfMap.get(subject));
				}
				// if no user specified URI, use generic custombaseURI
				else
				{
					idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ subject;
					conceptURIHash.put(subject, idxURI);
				}

				propURI = basePropURI+"/" + prop;
				createStatement(vf.createURI(propURI),RDF.TYPE,vf.createURI(basePropURI));
				basePropURIHash.put(prop,  propURI);
			}
		}
	}

	/**
	 * Create and store relationship property URIs at the SEMOSS base and instance levels 
	 */
	public void processRelationPropURIs() throws Exception
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
				if(headException = false)
					throw new Exception();
				String propURI = "";
				propURI = basePropURI+"/"+prop;
				createStatement(vf.createURI(propURI),RDF.TYPE,vf.createURI( basePropURI));
				basePropURIHash.put(prop,  propURI);
			}
		}
	}

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
		return output;
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
	 * Retrieves the data in the CSV file for a specified string
	 * @param object 	String containing the object to retrieve from the CSV data
	 * @param jcrMap 	Map containing the data in the CSV file
	 * @return Object	The CSV data mapped to the object string
	 */
	public Object createObject(String object, Map <String, Object> jcrMap)
	{
		// need to do the class vs. object magic
		return jcrMap.get(object);
	}

	/**
	 * Setter to store the metamodel created by user as a Hashtable
	 * @param data	Hashtable<String, String> containing all the information in a properties file
	 */
	public void setRdfMap(Hashtable<String, String> rdfMap) {
		this.rdfMap = rdfMap;
		propFileExist = false;
	}

	/**
	 * Load the CSV file
	 * Gets the headers for each column and reads the property file
	 * @param fileName String
	 */
	public void openCSVFile(String fileName) throws Exception
	{
		mapReader = new CsvMapReader(new FileReader(fileName), CsvPreference.STANDARD_PREFERENCE);		
		// store the headers of each of the columns
		header = mapReader.getHeader(true);
		headerList = Arrays.asList(header);
		// last header in CSV file is the absolute path to the prop file
		propFile = header[header.length-1];


	}

}