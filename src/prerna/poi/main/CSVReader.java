package prerna.poi.main;

import java.io.FileInputStream;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
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

import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

public class CSVReader {

	String fileName = null; // the file to be read and imported
	String propFile = null; // the file that serves as the property file
	Properties ontoProp = null;
	String bdPropFile = null;
	ICsvMapReader mapReader = null;
	String [] header = null;
	CellProcessor[] processors = null;
	Properties bdProp = new Properties();// properties for big data
	Sail bdSail = null;
	ValueFactory vf = null;
	static Hashtable <String, CellProcessor> typeHash = new Hashtable<String, CellProcessor>(); 
	Logger logger = Logger.getLogger(getClass());
	public SailConnection sc = null;
	public static Hashtable <String, String> createdNodes = new Hashtable<String, String>();
	
	public static String NUMCOL = "NUM_COLUMNS";
	public static String NOT_OPTIONAL = "NOT_OPTIONAL";
	
	
	public static void main(String[] args)
	{
		CSVReader reader = new CSVReader();
		reader.bdPropFile = "db/CBPclean.smss";
		reader.propFile = "db/CBPclean/CBP.prop";
		reader.fileName = "db/CBPclean/Cleansed_SEMOSS_v2.csv";
		reader.importCSV();
	}
	
	
	public void importCSV() 
	{
		
		try {
			// load the onto Prop file first
			openProp(propFile);
			// load the CSV reader next
			openCSVFile(fileName);
			// load the big data properties file
			loadBDProperties(bdPropFile);
			// create processors based on property file
			createProcessors();
			// DB
			openDB();
			// Process
			processRelationShips("RELATION");
			//close
			closeDB();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void createTypes()
	{
		typeHash.put("DECIMAL", new ParseDouble());
		typeHash.put("STRING", new NotNull());
		typeHash.put("DATE", new ParseDate("MM/dd/yyyy"));
		typeHash.put("NUMBER", new ParseInt());
		typeHash.put("BOOLEAN", new ParseBool());
		
		// now the optionals
		typeHash.put("DECIMAL_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("STRING_OPTIONAL", new Optional());
		typeHash.put("DATE_OPTIONAL", new Optional(new ParseDate("MM/dd/yyyy")));
		typeHash.put("NUMBER_OPTIONAL", new Optional(new ParseInt()));
		typeHash.put("BOOLEAN_OPTIONAL", new Optional(new ParseBool()));
		
	}
	
	public void createProcessors()
	{
		// get the number columns first
		int numColumns = Integer.parseInt(ontoProp.getProperty(NUMCOL));
		String optional  = ontoProp.getProperty(NOT_OPTIONAL);
		processors = new CellProcessor[numColumns];
		for(int procIndex = 0;procIndex < numColumns;procIndex++)
		{
			// find the type
			String type = ontoProp.getProperty(procIndex+"");
			boolean opt = true;
			if(optional.indexOf(";" + procIndex + ";") > 1)
				opt = false;
			
			if(type != null && opt)
				processors[procIndex] = typeHash.get(type.toUpperCase() + "_OPTIONAL");
			else if(type != null)
				processors[procIndex] = typeHash.get(type.toUpperCase());
			else if(type == null)
				processors[procIndex] = typeHash.get("STRING_OPTIONAL");
			
		}
	}
	
	
	public void processRelationShips(String relationName) throws Exception
	{
		// get all the relation
		String relationNames = ontoProp.getProperty(relationName);
        Map<String, Object> jcrMap;
    	//System.out.println("Processing Row " + mapReader.getRowNumber());
        int count = 0;
        int maxRows = 50000;

        while( (jcrMap = mapReader.read(header, processors)) != null && count<maxRows)
        {
        	System.out.println(count);
        	count++;
            StringTokenizer relationTokens = new StringTokenizer(relationNames, ";");
            for(int relIndex = 0;relationTokens.hasMoreElements();relIndex++)
            {
            	String relation = relationTokens.nextToken();

            	//System.out.println("Loading relation " + relation);

            	String subject = relation.substring(0,relation.indexOf("@"));
            	String object = relation.substring(relation.indexOf("@")+1);

            	// find the URI
            	String sURI = ontoProp.getProperty(subject);
            	String oURI = ontoProp.getProperty(object);
            	String relURI = ontoProp.getProperty(relation);    
            	
    
            	// composite URI
            	String sInstance = createInstanceURI(subject, jcrMap); // subject instance
            	Object oInstance = null;
            	if(oURI != null) // this would be a property
            		oInstance = createInstanceURI(object, jcrMap); // Object instance
            	else // this is a property
            		oInstance = createObject(object, jcrMap);

            	// create the concept
            	if(oURI != null)
            		createRelation(sInstance, oInstance+"", sURI, oURI, relURI);
            	// create the property
            	else if(oInstance != null)
            	{
            		// also try to check if the relation is null
            		if(relURI == null || relURI.length() == 0)
            			relURI = ontoProp.getProperty("Contains") + "/" + object;
            		createProperty(sInstance, oInstance, sURI, relURI);
            	}
            }
        }
	}

	protected String cleanString(String original, boolean replaceForwardSlash){
		String retString = original;
		retString = retString.trim();
		retString = retString.replaceAll(" ", "_");//replace spaces with underscores
		retString = retString.replaceAll("\"", "'");//replace double quotes with single quotes
		if(replaceForwardSlash)retString = retString.replaceAll("/", "-");//replace forward slashes with dashes
		retString = retString.replaceAll("\\|", "-");//replace vertical lines with dashes
		
		boolean doubleSpace = true;
		while (doubleSpace == true)//remove all double spaces
		{
			doubleSpace = retString.contains("  ");
			retString = retString.replace("  ", " ");
		}
		
		return retString;
	}
	
	public void closeDB() throws Exception
	{
		System.out.println("Closing....");
		//ng.stopTransaction(Conclusion.SUCCESS);
        InferenceEngine ie = ((BigdataSail)bdSail).getInferenceEngine();
        ie.computeClosure(null);

		sc.commit();
		sc.close();
		bdSail.shutDown();
		//ng.shutdown();
	}

	
	public void createProperty(String sInstance, Object oInstance,
			String sURI, String relURI) throws Exception
	{
		//System.out.println("Property >> " + sURI + "<>" + relURI + "<>"+oInstance);
		
		String subjectURI = sURI;
		String relationURI = relURI;
		
		if(!createdNodes.containsKey(subjectURI))
		{
			//createStatement(vf.createURI(subjectURI), RDF.TYPE, vf.createURI("http://www.w3.org/2000/01/rdf-schema#Class"));
			//createStatement(vf.createURI(subjectURI), RDFS.SUBCLASSOF, vf.createURI("http://www.w3.org/2000/01/rdf-schema#Class"));
			createdNodes.put(subjectURI, subjectURI);
		}
		if(!createdNodes.containsKey(relationURI))
		{
			//createStatement(vf.createURI(relationURI), RDF.TYPE, vf.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"));
			//createStatement(vf.createURI(relationURI), RDFS.SUBPROPERTYOF, vf.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"));
			createdNodes.put(relationURI, relationURI);
		}

		// create the subject first
		String sInstanceURI = sURI + "/" + sInstance;
		createStatement(vf.createURI(sInstanceURI), RDF.TYPE, vf.createURI(subjectURI));
		// create sInstanceURI <Typeof> subjectURI
		
		// now the relationship
		String relInstanceURI = relationURI + "/" + sInstance + ":" + oInstance;
		// create relationURI subpropertyof relationURI
		createStatement(vf.createURI(relURI), RDF.TYPE, vf.createURI(ontoProp.getProperty("Contains")));
		
		// create oInstanceURI <Typeof> objectURI - create a literal out of it

		// do the property magic here
		if(oInstance.getClass() == new Double(1).getClass())
		{
			//System.out.println("Found Double " + oInstance);
			createStatement(vf.createURI(sInstanceURI), vf.createURI(relURI), vf.createLiteral(((Double)oInstance).doubleValue()));
		}
		else if(oInstance.getClass() == new Date(1).getClass())
		{
			//System.out.println("Found Date " + oInstance);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			String date = df.format(oInstance);
			URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
			createStatement(vf.createURI(sInstanceURI), vf.createURI(relURI), vf.createLiteral(date, datatype));
		}
		else
		{
			//System.out.println("Found String " + oInstance);
			String value = oInstance + "";
			// try to see if it already has properties then add to it
			String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
			createStatement(vf.createURI(sInstanceURI), vf.createURI(relURI), vf.createLiteral(cleanValue));
		}				
	}
	
	public void loadBDProperties(String fileName) throws Exception
	{
		bdProp.load(new FileInputStream(fileName));

	}

	public void openDB() throws Exception {

		bdSail = new BigdataSail(bdProp);
		Repository repo = new BigdataSailRepository((BigdataSail) bdSail);
		repo.initialize();
		SailRepositoryConnection src = (SailRepositoryConnection) repo.getConnection();
		sc = src.getSailConnection();
		vf = bdSail.getValueFactory();

	}
	
	protected void createStatement(URI subject, URI predicate, Value object) throws Exception
	{
		//System.out.println("TRIPLE --  " + subject + "<>" + predicate + "<>" + object);
		
		URI newSub = null;
		URI newPred = null;
		Value newObj = null;
		String subString = null;
		String predString = null;
		String objString = null;
		String sub = subject.stringValue().trim();
		String pred = predicate.stringValue().trim();
				
		subString = cleanString(sub, false);
		newSub = vf.createURI(subString);
		
		predString = cleanString(pred, false);
		newPred = vf.createURI(predString);
		
		if(object instanceof Literal) 
			newObj = object;
		else {
			objString = cleanString(object.stringValue(), false);
			newObj = vf.createURI(objString);
		}
		
		sc.addStatement(newSub, newPred, newObj);
		
	}


	public String createInstanceURI(String subject, Map <String, Object> jcrMap)
	{
		String retString = null;
		
		StringTokenizer sTokens = new StringTokenizer(subject, "_");
		while(sTokens.hasMoreElements())
		{
			String token = sTokens.nextToken();
			if(jcrMap.containsKey(token) && jcrMap.get(token)!= null)
			{
				String value = jcrMap.get(token) + "";
				value = value.trim();
				value = cleanString(value, true);
				if(retString == null)
					retString = value;
				else
					retString = retString + "_" + value;
			}
		}
		//System.out.println(subject + "<>" + retString);
		return retString==null?null:retString;
	}
	
	public Object createObject(String object, Map <String, Object> jcrMap)
	{
		// need to do the class vs. object magic
		return jcrMap.get(object);
	}
	
	public void createRelation(String subject, String object, String subjectURI, String objectURI, String relationURI) throws Exception
	{
		// do nothing for now
		//System.out.println("Subject " + subjectURI + "/" + subject);
		//System.out.println("Object " + objectURI + "/" + object);
		//System.out.println("Relation " + relationURI + "/" + subject + ":" + object);
		
		// create the subject first
		String sInstanceURI = subjectURI + "/" + subject;
		// try to see if the base relations needs to be created
		if(!createdNodes.containsKey(subjectURI))
		{
			//createStatement(vf.createURI(subjectURI), RDF.TYPE, vf.createURI("http://www.w3.org/2000/01/rdf-schema#Class"));
			//createStatement(vf.createURI(subjectURI), RDFS.SUBCLASSOF, vf.createURI("http://www.w3.org/2000/01/rdf-schema#Class"));
			createdNodes.put(subjectURI, subjectURI);
		}
		if(!createdNodes.containsKey(objectURI))
		{
			//createStatement(vf.createURI(objectURI), RDF.TYPE, vf.createURI("http://www.w3.org/2000/01/rdf-schema#Class"));
			//createStatement(vf.createURI(objectURI), RDFS.SUBCLASSOF, vf.createURI("http://www.w3.org/2000/01/rdf-schema#Class"));
			createdNodes.put(objectURI, objectURI);
		}
		if(!createdNodes.containsKey(relationURI))
		{
			//createStatement(vf.createURI(relationURI), RDF.TYPE, vf.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"));
			//createStatement(vf.createURI(relationURI), RDFS.SUBPROPERTYOF, vf.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"));
			createdNodes.put(relationURI, relationURI);
		}

		
		// create sInstanceURI <Typeof> subjectURI
		createStatement(vf.createURI(sInstanceURI), RDF.TYPE, vf.createURI(subjectURI));
		// same thing with object
		String oInstanceURI = objectURI + "/" + object;
		// create oInstanceURI <Typeof> objectURI
		createStatement(vf.createURI(oInstanceURI), RDF.TYPE, vf.createURI(objectURI));
		
		// now the relationship
		String relInstanceURI = relationURI + "/" + subject + ":" + object;
		// create relationURI subpropertyof relationURI
		createStatement(vf.createURI(relInstanceURI), RDFS.SUBPROPERTYOF, vf.createURI(relationURI));

		// now that the basic is done
		// create the final relation
		//createStatement(vf.createURI(sInstanceURI), vf.createURI(relationURI), vf.createURI(oInstanceURI));
		createStatement(vf.createURI(sInstanceURI), vf.createURI(relInstanceURI), vf.createURI(oInstanceURI));
		
	}
	
	public void openProp(String fileName) throws Exception
	{
		ontoProp = new Properties();
		ontoProp.load(new FileInputStream(fileName));
	}
	
	public void openCSVFile(String fileName) throws Exception
	{
		mapReader = new CsvMapReader(new FileReader(fileName), CsvPreference.STANDARD_PREFERENCE);		
		header = mapReader.getHeader(true);
        createProcessors();
	}
}
