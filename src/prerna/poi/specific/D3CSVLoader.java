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
package prerna.poi.specific;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
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

import prerna.error.HeaderClassException;
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
public class D3CSVLoader {

	public final static String CONTAINS = "Contains";
	public final static String NUMCOL = "NUM_COLUMNS";
	public final static String NOT_OPTIONAL = "NOT_OPTIONAL";
	
	private static final Logger logger = LogManager.getLogger(D3CSVLoader.class.getName());
	
	private Properties rdfMap;
	private ICsvMapReader mapReader;
	private String [] header;
	private List<String> headerList;
	private CellProcessor[] processors;
	private Properties bdProp = new Properties(); // properties for big data
	private Sail bdSail;
	private ValueFactory vf;
	private SailConnection sc;
	private Hashtable <String, CellProcessor> typeHash = new Hashtable<String, CellProcessor>();
	private String customBaseURI = "";
	private ArrayList<String> relationArrayList, nodePropArrayList, relPropArrayList;
	private int count = 0;
	
	public String semossURI;
	public Hashtable<String,String> baseConceptURIHash = new Hashtable<String,String>(); 
	public Hashtable<String,String> conceptURIHash = new Hashtable<String,String>();
	public Hashtable<String,String> baseRelationURIHash = new Hashtable<String,String>(); 
	public Hashtable<String,String> relationURIHash = new Hashtable<String,String>();
	public Hashtable<String,String> basePropURIHash = new Hashtable<String,String>();
	public String basePropURI= "";

	// OWL variables
	private RepositoryConnection rcOWL;
	private SailConnection scOWL;
	private String owlFile;
	private Hashtable <String, String>uriHash = new Hashtable<String, String>();

	/**
	 * The main method is never called within SEMOSS
	 * Used to load data without having to start SEMOSS
	 * User must specify location of all files manually inside the method
	 * @param args String[]
	 */
	public static void main(String[] args) throws Exception
	{
		D3CSVLoader reader = new D3CSVLoader();
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		reader.customBaseURI = "http://health.mil/ontologies";
		reader.semossURI = "http://semoss.org/ontologies";
		reader.createTypes();
		String bdPropFile = workingDir + "/db/D3.smss";
		reader.owlFile = workingDir + "/db/D3/D3.OWL";

		String propFile = workingDir + "/RDF_Map.prop";
		DIHelper.getInstance().loadCoreProp(propFile);

		PropertyConfigurator.configure(workingDir + "/log4j.prop");

		reader.loadBDProperties(bdPropFile);
		reader.openDB();

		reader.openOWLWithOutConnection();
		ArrayList<String> files = new ArrayList<String>();
		files.add(workingDir+System.getProperty("file.separator")+"db"+System.getProperty("file.separator")+"D3"+System.getProperty("file.separator")+"Ships2.csv");
		for(int i = 0; i<files.size();i++)
		{
			String fileName = files.get(i);
			reader.openCSVFile(fileName);
			reader.createProcessors();
			reader.createURIHash();
			reader.processD3Relationships();
			reader.insertBaseRelations();
		}
		reader.closeDB();
	}

	public void createURIHash()
	{
		uriHash.put("Organization", "http://semoss.org/ontologies/Concept/Organization");
		uriHash.put("Geo", "http://semoss.org/ontologies/Concept/Location");
		uriHash.put("RGeo", "http://semoss.org/ontologies/Relation/LocatedAt");
		uriHash.put("Person (Generic)", "http://semoss.org/ontologies/Concept/Person");
		uriHash.put("executive of", "http://semoss.org/ontologies/Relation/Executive");
		uriHash.put("telephone number of", "http://semoss.org/ontologies/Relation/Contains/Tel");
		uriHash.put("fax number of", "http://semoss.org/ontologies/Relation/Contains/Fax");
		uriHash.put("DUNS number of", "http://semoss.org/ontologies/Relation/Contains/DUNS");
		uriHash.put("address of", "http://semoss.org/ontologies/Relation/Contains/Address");
		uriHash.put("employee of", "http://semoss.org/ontologies/Relation/Employee");
		uriHash.put("subsidiary of", "http://semoss.org/ontologies/Relation/Subsidiary");
		uriHash.put("website of", "http://semoss.org/ontologies/Relation/Contains/Website");	
		uriHash.put("latitude of", "http://semoss.org/ontologies/Relation/Contains/Latitude");
		uriHash.put("longitude of", "http://semoss.org/ontologies/Relation/Contains/Longitude");
	}

	public void insertBaseRelations()
	{
		// add all the concepts
		try {
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Concept/Organization"), RDFS.SUBCLASSOF, vf.createURI("http://semoss.org/ontologies/Concept"));

			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Concept/Person"), RDFS.SUBCLASSOF, vf.createURI("http://semoss.org/ontologies/Concept"));
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Concept/Location"), RDFS.SUBCLASSOF, vf.createURI("http://semoss.org/ontologies/Concept"));

			// add all the relations
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Executive"), RDFS.SUBPROPERTYOF, vf.createURI("http://semoss.org/ontologies/Relation"));
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Employee"), RDFS.SUBPROPERTYOF, vf.createURI("http://semoss.org/ontologies/Relation"));
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Subsidiary"), RDFS.SUBPROPERTYOF, vf.createURI("http://semoss.org/ontologies/Relation"));
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/LocatedAt"), RDFS.SUBPROPERTYOF, vf.createURI("http://semoss.org/ontologies/Relation"));

			// add all properties
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/DUNS"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Address"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Website"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Tel"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Fax"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Latitude"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			scOWL.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Longitude"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));


			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Concept/Organization"), RDFS.SUBCLASSOF, vf.createURI("http://semoss.org/ontologies/Concept"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Concept/Person"), RDFS.SUBCLASSOF, vf.createURI("http://semoss.org/ontologies/Concept"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Concept/Location"), RDFS.SUBCLASSOF, vf.createURI("http://semoss.org/ontologies/Concept"));

			// add all the relations
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Executive"), RDFS.SUBPROPERTYOF, vf.createURI("http://semoss.org/ontologies/Relation"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Employee"), RDFS.SUBPROPERTYOF, vf.createURI("http://semoss.org/ontologies/Relation"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Subsidiary"), RDFS.SUBPROPERTYOF, vf.createURI("http://semoss.org/ontologies/Relation"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/LocatedAt"), RDFS.SUBPROPERTYOF, vf.createURI("http://semoss.org/ontologies/Relation"));			

			// add all properties
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/DUNS"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Address"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Website"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Tel"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Fax"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Latitude"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));
			sc.addStatement(vf.createURI("http://semoss.org/ontologies/Relation/Contains/Longitude"), RDF.TYPE, vf.createURI("http://semoss.org/ontologies/Relation/Contains"));

			FileWriter fWrite = new FileWriter(owlFile);
			RDFXMLPrettyWriter owlWriter  = new RDFXMLPrettyWriter(fWrite); 
			rcOWL.export(owlWriter);
			fWrite.close();
			owlWriter.close();
		} catch (SailException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			e.printStackTrace();
		}
	}

	public void processD3Relationships()
	{
		try {
			Map<String, Object> csvMap;
			while( (csvMap = mapReader.read(header, processors)) != null)
			{
				// get the column called object
				String object = (String)csvMap.get("Object");
				String objectType = (String)csvMap.get("Type");
				String secondaryObject = (String)csvMap.get("Type");
				String predicate = (String)csvMap.get("Predicate");
				String subject = (String) csvMap.get("Subject");
				String subjectTypeURI = uriHash.get("Organization");
				String subjectURI = subjectTypeURI + "/" + Utility.cleanString(subject, true);
				if(predicate != null && uriHash.containsKey(predicate))
				{
					String predicateTypeURI = uriHash.get(predicate);
					String predicateURI = predicateTypeURI + "/" + Utility.cleanString(subject, false) + "_" + Utility.cleanString(object, true);
					printTriple(predicateURI, RDFS.SUBPROPERTYOF +"" , predicateTypeURI);
					if(predicate.contains("address"))
					{
						objectType = "Location";
						printTriple(subjectURI, uriHash.get("RGeo") +"/" + Utility.cleanString(subject, true)+ "_" + Utility.cleanString(secondaryObject, true), uriHash.get("Geo") + "/" + Utility.cleanString(secondaryObject, true));
						printTriple(uriHash.get("RGeo") +"/" + Utility.cleanString(subject, true)+ "_" + Utility.cleanString(secondaryObject, true), RDFS.SUBPROPERTYOF+"", uriHash.get("RGeo"));
						printTriple(uriHash.get("Geo") + "/" + Utility.cleanString(secondaryObject, true), RDF.TYPE+"", uriHash.get("Geo"));
					}
					if(objectType != null && uriHash.containsKey(objectType))
					{
						String objectTypeURI = uriHash.get(objectType);
						String objectURI = objectTypeURI + "/" + Utility.cleanString(object, true);
						// object type
						printTriple(subjectURI, RDF.TYPE + "", subjectTypeURI);
						printTriple(objectURI, RDF.TYPE+"", objectTypeURI);
						printTriple(objectURI, RDFS.LABEL+"", new LiteralImpl(object));
						// the core triple
						//printTriple(objectURI, predicateURI, subjectURI);
						printTriple(subjectURI, predicateURI, objectURI);
						//printTriple(object, predicate, subject);
					}
					else
						printTriple(subjectURI, predicateURI, new LiteralImpl(object));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void printTriple(String subject, String predicate, Object object)
	{
		logger.error(subject + "<>" + predicate + "<>" + object);
		try {
			if(object instanceof Literal)
				sc.addStatement(vf.createURI(subject), vf.createURI(predicate), (Literal)object);
			else
				sc.addStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object+""));
		} catch (SailException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Loading data into SEMOSS to create a new database
	 * @param dbName 		String grabbed from the user interface that would be used as the name for the database
	 * @param fileNames		Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase	String grabbed from the user interface that is used as the URI base for all instances 
	 * @param customMap		
	 * @param owlFile		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws RepositoryException 
	 * @throws RDFHandlerException 
	 * @throws SailException 
	 * @throws HeaderClassException 
	 */
	public void importFileWithOutConnection(String dbName, String fileNames, String customBase, String owlFile) throws FileNotFoundException, IOException, RepositoryException, SailException, RDFHandlerException, HeaderClassException  
	{
		String[] files = fileNames.split(";");
		this.semossURI = (String) DIHelper.getInstance().getLocalProp(Constants.SEMOSS_URI);
		//make location of the owl file in the dbname folder
		this.owlFile = owlFile; 
		String bdPropFile = dbName;
		if(!customBase.equals(""))
		{
			customBaseURI = customBase;
		}
		semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		createTypes();
		loadBDProperties(bdPropFile);
		openDB();
		openOWLWithOutConnection();
		for(int i = 0; i<files.length;i++)
		{
			String fileName = files[i];
			openCSVFile(fileName);			
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
	 * @throws RepositoryException 
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public void importFileWithConnection(String engineName, String fileNames, String customBase, String owlFile) throws RepositoryException, FileNotFoundException, IOException  
	{
		String[] files = fileNames.split(";");
		semossURI = (String) DIHelper.getInstance().getLocalProp(Constants.SEMOSS_URI);
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
		BigDataEngine bigEngine = (BigDataEngine) engine;
		//make location of the owl file in the dbname folder
		this.owlFile = owlFile; 
		semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		if(!customBase.equals("")) customBaseURI = customBase;
		bdSail = bigEngine.bdSail;
		sc = bigEngine.sc;
		vf = bigEngine.vf;
		openOWLWithConnection(engine);
		createTypes();
		for(int i = 0; i<files.length;i++)
		{
			openCSVFile(files[i]);
			// load the big data properties file
			// create processors based on property file
			createProcessors();
			// DB
			// Process
			createURIHash();
			processD3Relationships();
			insertBaseRelations();
		}
		bigEngine.infer();
	}

	/**
	 * Creates all base relationships in the metamodel to add into the database and creates the OWL file
	 * @throws SailException 
	 * @throws IOException 
	 * @throws RDFHandlerException 
	 * @throws RepositoryException 
	 */
	public void createBaseRelations() throws SailException, IOException, RepositoryException, RDFHandlerException 
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

		if(basePropURI.equals("")){
			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		}
		scOWL.addStatement(vf.createURI(basePropURI), vf.createURI(Constants.SUBPROPERTY_URI), vf.createURI(basePropURI));

		Iterator<String> baseHashIt = baseConceptURIHash.keySet().iterator();
		//now add all of the base relations that have been stored in the hash.
		while(baseHashIt.hasNext()){
			String subjectInstance = baseHashIt.next() +"";
			String predicate = Constants.SUBCLASS_URI;
			//convert instances to URIs
			String subject = baseConceptURIHash.get(subjectInstance) +"";
			String object = semossURI + "/Concept";
			// create the statement now
			createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			// add base relations URIs to OWL
			scOWL.addStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			scOWL.commit();
		}
		baseHashIt = baseRelationURIHash.keySet().iterator();
		while(baseHashIt.hasNext()){
			String subjectInstance = baseHashIt.next() +"";
			String predicate = Constants.SUBPROPERTY_URI;
			//convert instances to URIs
			String subject = baseRelationURIHash.get(subjectInstance) +"";
			String object = semossURI + "/Relation";
			// create the statement now
			createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			// add base relationship URIs to OWL
			scOWL.addStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			scOWL.commit();
		}

		// create the OWL File
		FileWriter fWrite = new FileWriter(owlFile);
		RDFXMLPrettyWriter owlWriter  = new RDFXMLPrettyWriter(fWrite); 
		rcOWL.export(owlWriter);
		fWrite.close();
		owlWriter.close();

		closeOWL();
	}

	/**
	 * Stores all possible variable types that the user can input from the CSV file into hashtable
	 * Hashtable is then used to match each column in CSV to a specific type based on user input in prop file
	 */
	public void createTypes()
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
		int numColumns = Integer.parseInt(rdfMap.getProperty(NUMCOL));
		// Columns in prop file that are NON_OPTIMAL must contain a value
		String optional  = rdfMap.getProperty(NOT_OPTIONAL);
		processors = new CellProcessor[numColumns];
		for(int procIndex = 1;procIndex <= numColumns;procIndex++)
		{
			// find the type for each column
			String type = rdfMap.getProperty(procIndex+"");
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
	 * @throws IOException 
	 */
	public void skipRows() throws IOException
	{
		//start count at 1 just row 1 is the header
		count = 1;
		int startRow = 2;
		if (rdfMap.getProperty("START_ROW") != null)
			startRow = Integer.parseInt(rdfMap.getProperty("START_ROW")); 
		while( count<startRow-1 && mapReader.read(header, processors) != null)// && count<maxRows)
		{
			count++;
			logger.info("Skipping line: " + count);
		}
	}

	/**
	 * Create all the triples associated with the relationships specified in the prop file
	 * @throws SailException 
	 * @throws IOException 
	 */
	public void processRelationShips() throws SailException, IOException
	{
		// get all the relation
		Map<String, Object> jcrMap;
		// max row predetermined value
		int maxRows = 10000;
		// overwrite this value if user specified the max rows to load
		if (rdfMap.getProperty("END_ROW") != null)
			maxRows =  Integer.parseInt(rdfMap.getProperty("END_ROW"));
		// only start from the maxRow - the startRow
		// added -1 is because of index nature
		// the earlier rows should already have been skipped
		while( (jcrMap = mapReader.read(header, processors)) != null && count<(maxRows))
		{
			count++;
			logger.info("Process line: " +count);

			// process all relationships in row
			for(int relIndex = 0;relIndex<relationArrayList.size();relIndex++)
			{
				String relation = relationArrayList.get(relIndex);
				String[] strSplit = relation.split("@");
				// get the subject and object for triple (the two indexes)
				String subject = strSplit[0];
				String predicate = strSplit[1];
				String object = strSplit[2];
				String relPropString = subject + "_"+ predicate + "_" + object;

				String subjectValue = createInstanceValue(subject, jcrMap);
				String objectValue = createInstanceValue(object, jcrMap);
				if (subjectValue.equals("") || objectValue.equals(""))
				{
					continue;
				}

				//get all uri's needed for given relationship
				String subjectInstanceURI = conceptURIHash.get(subject)+"/"+subjectValue;
				String objectInstanceURI = conceptURIHash.get(object)+"/"+objectValue;
				String subjectTypeURI = baseConceptURIHash.get(subject+Constants.CLASS);
				String objectTypeURI = baseConceptURIHash.get(object+Constants.CLASS);

				// need to do a check to find if the predicate is dynamic
				if(predicate.startsWith("dynamic"))
				{
					System.err.println("This is dynamic");
					// and then compose the URI
				}

				String predicateInstanceURI = relationURIHash.get(relPropString)+"/"+subjectValue+Constants.RELATION_URI_CONCATENATOR+objectValue;
				String predicateSubclassURI = baseRelationURIHash.get(relPropString+Constants.CLASS);

				//creates seven triples (three of which are label triples)
				createStatement(vf.createURI(subjectInstanceURI), RDF.TYPE, vf.createURI(subjectTypeURI));
				createStatement(vf.createURI(objectInstanceURI), RDF.TYPE, vf.createURI(objectTypeURI));
				createStatement(vf.createURI(subjectInstanceURI), vf.createURI(predicateInstanceURI), vf.createURI(objectInstanceURI));
				createStatement(vf.createURI(predicateInstanceURI), RDFS.SUBPROPERTYOF, vf.createURI(predicateSubclassURI));
				createStatement(vf.createURI(subjectInstanceURI), RDFS.LABEL, vf.createLiteral(subjectValue));
				createStatement(vf.createURI(objectInstanceURI), RDFS.LABEL, vf.createLiteral(objectValue));
				createStatement(vf.createURI(predicateInstanceURI), RDFS.LABEL, vf.createLiteral(subjectValue+Constants.RELATION_URI_CONCATENATOR+objectValue));

				// look through all relationship properties for the specific relationship
				for(int relPropIndex = 0; relPropIndex < relPropArrayList.size(); relPropIndex++)
				{
					String relProp = relPropArrayList.get(relPropIndex);
					String[] relPropSplit = relProp.split("%");
					if(relPropSplit[0].equals(relation))
					{
						// loop through all properties on the relationship
						for(int i = 1; i < relPropSplit.length; i++)
						{
							String propURI = basePropURIHash.get(relPropSplit[i]);
							// add the necessary triples for the relationship property
							createProperty(predicateInstanceURI, propURI, relPropSplit[i], jcrMap);
						}
					}
				}
			}

			// look through all node properties
			for(int relIndex = 0;relIndex<nodePropArrayList.size();relIndex++)
			{
				String relation = nodePropArrayList.get(relIndex);
				String[] strSplit = relation.split("%");
				// get the subject (the first index) and objects for triple
				String subject = strSplit[0];
				String subjectValue = createInstanceValue(subject, jcrMap);
				String subjectInstanceURI = conceptURIHash.get(subject)+"/"+subjectValue;
				// loop through all properties on the node
				for(int i = 1; i < strSplit.length; i++)
				{
					String prop = strSplit[i];
					String propURI = basePropURIHash.get(prop);
					createStatement(vf.createURI(subjectInstanceURI), RDF.TYPE, vf.createURI(baseConceptURIHash.get(subject+Constants.CLASS)));
					createProperty(subjectInstanceURI, propURI,prop, jcrMap);
				}
			}
		}
	}

	/**
	 * Create and store concept and relation URIs at the SEMOSS base and instance levels
	 * @throws HeaderClassException 
	 */
	public void processConceptRelationURIs() throws HeaderClassException
	{
		// get the list of relationships from the prop file
		String relationNames = rdfMap.getProperty("RELATION");
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

			// check to see if the predicate has 
			if(predicate.startsWith("dynamic"))
				System.err.println("Creating Dynamic ");

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
			if(headException == false)
				throw new HeaderClassException(subject + " cannot be found as a header");

			if(object.contains("+"))
			{
				headException = isProperConcatHeader(object);
			}
			else
			{
				if(!headerList.contains(object))
					headException = false;
			}
			if(headException == false)
				throw new HeaderClassException(subject + " cannot be found as a header");

			// create concept uris
			String relURI = "";
			String relBaseURI = "";
			String idxBaseURI = "";
			String idxURI = "";

			// see if subject node SEMOSS base URI exist in prop file first
			if(rdfMap.containsKey(subject+Constants.CLASS))
			{
				baseConceptURIHash.put(subject+Constants.CLASS,rdfMap.getProperty(subject+Constants.CLASS));
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
				conceptURIHash.put(subject, rdfMap.getProperty(subject));
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
				baseConceptURIHash.put(object+Constants.CLASS,rdfMap.getProperty(object+Constants.CLASS));
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
				conceptURIHash.put(object, rdfMap.getProperty(object));
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
				baseRelationURIHash.put(relPropString+Constants.CLASS,rdfMap.getProperty(relPropString+Constants.CLASS));
			}
			// if no user specified URI, use generic SEMOSS base URI
			else
			{
				relBaseURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + predicate;
				baseRelationURIHash.put(relPropString+Constants.CLASS, relBaseURI);
			}
			// see if relationship URI exists in prop file
			if(rdfMap.containsKey(relPropString)) {
				relationURIHash.put(relPropString,rdfMap.getProperty(relPropString));
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
	 * @throws SailException 
	 * @throws HeaderClassException 
	 */
	public void processNodePropURIs() throws SailException, HeaderClassException
	{
		String nodePropNames = rdfMap.getProperty("NODE_PROP");
		StringTokenizer nodePropTokens = new StringTokenizer(nodePropNames, ";");
		nodePropArrayList = new ArrayList<String>();
		if(basePropURI.equals("")){
			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		}
		createStatement(vf.createURI(basePropURI),vf.createURI(Constants.SUBPROPERTY_URI),vf.createURI(basePropURI));

		while(nodePropTokens.hasMoreElements())
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
				if(headException == false)
					throw new HeaderClassException(subject + " cannot be found as a header");

				if(prop.contains("+"))
				{
					headException = isProperConcatHeader(prop);
				}
				else
				{
					if(!headerList.contains(prop))
						headException = false;
				}
				if(headException == false)
					throw new HeaderClassException(subject + " cannot be found as a header");

				// see if subject node SEMOSS base URI exists in prop file
				if(rdfMap.containsKey(subject+Constants.CLASS))
				{
					baseConceptURIHash.put(subject+Constants.CLASS,rdfMap.getProperty(subject));
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
					conceptURIHash.put(subject, rdfMap.getProperty(subject));
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
	 * @throws HeaderClassException 
	 * @throws SailException 
	 */
	public void processRelationPropURIs() throws HeaderClassException, SailException
	{
		String propNames = rdfMap.getProperty("RELATION_PROP");
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
				if(headException == false)
					throw new HeaderClassException(prop + " cannot be found as a header");

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
	 * Creates and adds the triples associated with properties based on the variable type
	 * @param subjectURI 		String containing the instance URI of the node or relationship with the property
	 * @param propPredBaseURI 	String containing the URI of the relationship 
	 * @param propName 			String containing the name of the property
	 * @param jcrMap 			Map containing the data in the CSV file
	 * @throws SailException 
	 */
	public void createProperty(String subjectURI, String propPredBaseURI, String propName, Map<String, Object> jcrMap) throws SailException
	{
		if(jcrMap.containsKey(propName) && jcrMap.get(propName)!= null)
		{
			Object oInstance = createObject(propName, jcrMap);
			if(oInstance instanceof Double)
			{
				createStatement(vf.createURI(subjectURI), vf.createURI(propPredBaseURI), vf.createLiteral(((Double)oInstance).doubleValue()));
			}
			else if(oInstance instanceof Date)
			{
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				if(oInstance.toString().length()<=10)
				{
					df=new SimpleDateFormat("MM-dd-yyyy");
				}
				String date = df.format(oInstance);
				URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
				createStatement(vf.createURI(subjectURI), vf.createURI(propPredBaseURI), vf.createLiteral(date, datatype));
			}
			else
			{
				String value = oInstance + "";
				// try to see if it already has properties then add to it
				String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
				createStatement(vf.createURI(subjectURI), vf.createURI(propPredBaseURI), vf.createLiteral(cleanValue));
			}		
		}

	}

	/**
	 * Creates and adds the triple into the repository connection
	 * @param subject		URI for the subject of the triple
	 * @param predicate		URI for the predicate of the triple
	 * @param object		Value for the object of the triple, this param is not a URI since objects can be literals and literals do not have URIs
	 * @throws SailException 
	 */
	protected void createStatement(URI subject, URI predicate, Value object) throws SailException
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
		return jcrMap.get(object);
	}

	/**
	 * Loading engine properties in order to create the database 
	 * @param fileName String containing the fileName of the temp file that contains the information of the smss file
	 * @throws FileNotFoundException 
	 */
	public void loadBDProperties(String fileName) throws FileNotFoundException, IOException
	{
		InputStream fis = new FileInputStream(fileName);
		bdProp.load(fis);
		fis.close();
	}

	/**
	 * Creates the database based on the engine properties 
	 * @throws RepositoryException 
	 */
	public void openDB() throws RepositoryException 
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
	 * Loads the prop file for the CSV file
	 * @param fileName	Absolute path to the prop file specified in the last column of the CSV file
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public void openProp(String fileName) throws FileNotFoundException, IOException
	{
		rdfMap = new Properties();
		rdfMap.load(new FileInputStream(fileName));
	}

	/**
	 * Load the CSV file
	 * Gets the headers for each column and reads the property file
	 * @param fileName String
	 * @throws FileNotFoundException 
	 */
	public void openCSVFile(String fileName) throws FileNotFoundException, IOException
	{
		mapReader = new CsvMapReader(new FileReader(fileName), CsvPreference.STANDARD_PREFERENCE);		
		header = mapReader.getHeader(true);
		headerList = Arrays.asList(header);
		String propFileName = "db/d3/csvload.prop";
		openProp(propFileName);
		createProcessors();
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
		rcOWL.getValueFactory();
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
		rcOWL.getValueFactory();

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
	 * Close the OWL engine
	 * @throws SailException 
	 * @throws RepositoryException 
	 */
	protected void closeOWL() throws SailException, RepositoryException
	{
		scOWL.close();
		rcOWL.close();
	}

	/**
	 * Close the database engine
	 * @throws SailException 
	 */
	public void closeDB() throws SailException
	{
		logger.warn("Closing....");
		sc.commit();
		InferenceEngine ie = ((BigdataSail)bdSail).getInferenceEngine();
		ie.computeClosure(null);
		sc.commit();
		sc.close();
		bdSail.shutDown();
	}	
}