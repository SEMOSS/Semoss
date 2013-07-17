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
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.constraint.StrRegEx;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;




public class JCRReader {

	public static final String RELATION = "RELATION_NAMES";
	Properties ontoProp = null;
	ICsvMapReader mapReader = null;
	String [] header = null;
	CellProcessor[] processors = null;
	Properties bdProp = new Properties();// properties for big data
	Sail bdSail = null;
	ValueFactory vf = null;
	Logger logger = Logger.getLogger(getClass());
	public SailConnection sc = null;
	Hashtable <String, String> createdNodes = new Hashtable<String, String>();

	
	
	public static void main(String [] args) throws Exception
	{
		JCRReader reader = new JCRReader();
		
		//reader.readWithCsvBeanReader();
		//reader.readWithCsvMapReader();
		// load the database
		reader.loadBDProperties("db/Trueup.smss");
		reader.openDB();		
		reader.openCSVFile("sample2.csv");
		reader.openProp("db/Trueup/onto.prop");
		reader.processRelationShips();
		reader.closeDB();
	}
	
	public void processRelationShips() throws Exception
	{
		// get all the relation
		String relationNames = ontoProp.getProperty(RELATION);
        Map<String, Object> jcrMap;
    	System.out.println("Processing Row " + mapReader.getRowNumber());

        while( (jcrMap = mapReader.read(header, processors)) != null )
        {
            StringTokenizer relationTokens = new StringTokenizer(relationNames, ";");
            for(int relIndex = 0;relationTokens.hasMoreElements();relIndex++)
            {
            	String relation = relationTokens.nextToken();
            	String subject = relation.substring(0,relation.indexOf("@"));
            	String object = relation.substring(relation.indexOf("@")+1);

            	System.out.println("Loading relation " + relation);

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
            		createProperty(sInstance, oInstance, sURI, relURI);
            }
        }
	}

	private String cleanString(String original, boolean replaceForwardSlash){
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
		System.out.println("Property >> " + sURI + "<>" + relURI + "<>"+oInstance);
		
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
			System.out.println("Found Double " + oInstance);
			createStatement(vf.createURI(sInstanceURI), vf.createURI(relURI), vf.createLiteral(((Double)oInstance).doubleValue()));
		}
		else if(oInstance.getClass() == new Date(1).getClass())
		{
			System.out.println("Found Date " + oInstance);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			String date = df.format(oInstance);
			URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
			createStatement(vf.createURI(sInstanceURI), vf.createURI(relURI), vf.createLiteral(date, datatype));
		}
		else
		{
			System.out.println("Found String " + oInstance);
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
	
	private void createStatement(URI subject, URI predicate, Value object) throws Exception
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
		System.out.println(subject + "<>" + retString);
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
		System.out.println("Subject " + subjectURI + "/" + subject);
		System.out.println("Object " + objectURI + "/" + object);
		System.out.println("Relation " + relationURI + "/" + subject + ":" + object);
		
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
        processors = getProcessors();
	}
	
	
private static CellProcessor[] getProcessors() {
        
        final String emailRegex = "[a-z0-9\\._]+@[a-z0-9\\.]+"; // just an example, not very robust!
        StrRegEx.registerMessage(emailRegex, "must be a valid email address");
        
        final CellProcessor[] processors = new CellProcessor[] { 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), //10
                new Optional(),//11
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), //20
                new Optional(), //21
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(new ParseDouble()),// 26 // Direct Labor Rate 
                new Optional(new ParseDouble()), //27 // hours
                new Optional(), 
                new Optional(), 
                new Optional(new ParseDouble()), //30 - Fringe Rate
                new Optional(), //31
                new Optional(new ParseDouble()), // OH Rate
                new Optional(), 
                new Optional(new ParseDouble()), // GAA Rate
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new ParseDate("MM/dd/yyyy"), // date
                new Optional(), //40
                new Optional(), //41
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(), 
                new Optional(),
                new Optional(), 
                new Optional(),//50 
                new Optional()
                /*new ParseDate("dd/MM/yyyy"), // birthDate
                new Optional(), // mailingAddress
                new Optional(new ParseBool()), // married
                new Optional(new ParseInt()), // numberOfKids
                new Optional(), // favouriteQuote*/
                
        };
        
        return processors;
}	
}
