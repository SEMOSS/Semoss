package prerna.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.poi.main.BaseDatabaseCreator;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;

@Deprecated
public class OWLERLineage {

	// predefined URIs
	public static String BASE_URI = "http://semoss.org/ontologies/";
	public static final String SEMOSS_URI = BASE_URI;
	public static final String DEFAULT_LINEAGE_CLASS = "Lineage";
	public static final String DEFAULT_NODE_CLASS = "Concept";
	public static final String DEFAULT_RELATION_CLASS = "Relation";
 
	// hashtable of lineage relation
	private Hashtable<String, String> lineageRelationHash = new Hashtable<String, String>();
	
	// hashtable of lineage properties
	private Hashtable<String, String> lineagePropHash = new Hashtable<String, String>();
	
	// need to know the database type due to differences in URIs when the
	// database is RDF vs. RDBMS
	private IEngine.ENGINE_TYPE type = null;
	// the engine here is a wrapper around a RDFFileSesameEngine which helps with adding the URIs into the engine
	private BaseDatabaseCreator engine = null;
	// file name for the location of the OWL file to write to
	private String owlPath = null;
	
	/**
	 * Constructor for the class when we are creating a brand new OWL file
	 * @param fileName				The location of the new OWL file
	 * @param type					The type of the engine the OWL file is being created for
	 */
	public OWLERLineage(String owlPath, IEngine.ENGINE_TYPE type) {
		this.owlPath = owlPath;
		this.type = type;
		
		String baseSubject = BASE_URI + DEFAULT_NODE_CLASS ;
		String baseRelation = BASE_URI + DEFAULT_RELATION_CLASS;
		String predicate = RDF.TYPE.stringValue();
		
		this.engine = new BaseDatabaseCreator(owlPath);
		this.engine.addToBaseEngine(baseSubject, predicate, RDFS.CLASS.stringValue());
		this.engine.addToBaseEngine(baseRelation, predicate, RDF.PROPERTY.stringValue());
	}
	
	//TODO Open an existing OWL file and append the lineage information
	/**
	 * Constructor for the class when we are adding to an existing OWL file
	 * @param existingEngine		The engine we are adding to
	 * @param fileName				The location of the OWL file
	 */
	public OWLERLineage(IEngine existingEngine, String owlPath) {
		this.owlPath = owlPath;
		this.engine = new BaseDatabaseCreator(existingEngine, owlPath);
	}
	
	/**
	 * Closes the connection to the RDFFileSesameEngine supported by the OWL
	 */
	public void closeOwl() {
		engine.closeBaseEng();
	}
	
	/**
	 * Commits the modifications to the OWL file into the engine
	 */
	public void commit() {
		engine.commit();
	}
	
	/**
	 * Exports the information added into the OWL file located at the owlPath
	 * @throws IOException
	 */
	public void export() throws IOException {
		engine.exportBaseEng(true);
	}
	
	//////////////////ADDING LINEAGE RELATION INTO THE OWL /////////////////////////////////
	/*
	 * Add the Lineage relationship  
	 * The relationship is Table A -> Table A's Successor table
	 * The example below shows table CL_DEPOSIT_SERVICE -> table LDG_DEPOSIT_SERVICE
	 * <rdf:Description rdf:about="http://semoss.org/ontologies/Concept/CL_DEPOSIT_SERVICE">
	 *	<CL_DEPOSIT_SERVICE.LDG_DEPOSIT_SERVICE xmlns="http://semoss.org/ontologies/Lineage/" rdf:resource="http://semoss.org/ontologies/Concept/LDG_DEPOSIT_SERVICE"/>
	 *	</rdf:Description>


	/**
	 * Adding a Lineage relationship into the OWL
	 * There are some differences based on how the information is used based on if it is a 
	 * RDF engine or a RDBMS engine
	 * @param tableNameSource				Source table name
	 * 								
	 * @param tableNameTarget				Table successor
	 * 								 
	 */
	
	public String addLineageRelation(String tableNameSource, String tableNameTarget)
	{
 		if(!lineageRelationHash.containsKey(tableNameSource + "%" + tableNameTarget)) {
			String baseNodeURI = SEMOSS_URI + DEFAULT_NODE_CLASS;
			String baseLineageURI = SEMOSS_URI + DEFAULT_LINEAGE_CLASS;
			
			String tableSource = baseNodeURI + "/"+ tableNameSource;
			String tableTarget = baseNodeURI + "/"+ tableNameTarget;
			// create the predicate as tableSource.tableNameTarget
			String lineageRelationship = baseLineageURI + "/" + tableNameSource + "." + tableNameTarget;
			
			// save the relationship between the table source and table target
			engine.addToBaseEngine(tableSource, lineageRelationship, tableTarget);
			// define the relationship as a subproperty of lineage
			engine.addToBaseEngine(lineageRelationship, RDFS.SUBPROPERTYOF.stringValue(), baseLineageURI);

			// store it in the hash for future use
			lineageRelationHash.put(tableNameSource + "%" + tableNameTarget, lineageRelationship);
		}
		return lineageRelationHash.get(tableNameSource + "%" + tableNameTarget);
	}
	
	
	//////////////////ADDING LINEAGE PROPERTIES INTO THE OWL /////////////////////////////////
	
	/*
	 * Add the LINEAGE PROPERTIES  
	 * Transformation is the transformation type between two tables, it can be aggregation, calculation, landing, etc
	 * Layer defined as the lineage layer. for instance, report at the bottom level should be layer 1, and source table at the top level should has the largest layer number
	 *  The example below shows table CL_DEPOSIT_SERVICE -> table LDG_DEPOSIT_SERVICE
	 *  <rdf:Description rdf:about="http://semoss.org/ontologies/Lineage/CL_DEPOSIT_SERVICE.LDG_DEPOSIT_SERVICE">
		<rdfs:subPropertyOf rdf:resource="http://semoss.org/ontologies/Lineage"/>
		<Transformation xmlns="http://semoss.org/ontologies/Relation/" rdf:resource="http://semoss.org/ontologies/Lineage/Landing"/>
		<Layer xmlns="http://semoss.org/ontologies/Relation/" rdf:resource="http://semoss.org/ontologies/Lineage/3"/>
		</rdf:Description>


	/**
	 * Adding a Lineage relationship into the OWL
	 * There are some differences based on how the information is used based on if it is a 
	 * RDF engine or a RDBMS engine
	 * @param tableNameSource				Source table name 								
	 * @param tableNameTarget				Table successor
	 * @param trans							transformation type
	 * @param layer							lineage layer				 
	 */
	
	public String addLineageProp(String tableNameSource, String tableNameTarget, String prop, String value)
	{
		// since RDF uses this multiple times, don't create it each time and just store it in a hash to send back
		if(!lineagePropHash.containsKey(tableNameSource + "%" + tableNameTarget)) {
			String baseLineageURI = SEMOSS_URI + DEFAULT_LINEAGE_CLASS;
			String baseLineageProp = "LINEAGE:";

			// create the lineage relationship as tableNameSource.tableNameTarget
			String lineageRelationship = baseLineageURI + "/" + tableNameSource + "." + tableNameTarget;
			
			// Added the property w/ its value
			String propRelationship = baseLineageProp + prop;
			engine.addToBaseEngine(propRelationship, RDF.TYPE + "", "LINEAGE:PROPERTY");
			engine.addToBaseEngine(lineageRelationship, propRelationship, "LINEAGE:" + value);
			
			lineagePropHash.put(tableNameSource + "%" + tableNameTarget, lineageRelationship);
		}
		return lineagePropHash.get(tableNameSource + "%" + tableNameTarget);
	}
 
  
	/////////////////// END ADDITIONAL METHODS TO INSERT INTO THE OWL /////////////////////////////////

	
	///////////////////// TESTING /////////////////////
	public static void main(String [] args) throws Exception  {
		/*
		 *  Testing data
		SOURCE_TABLE	           TABLE_SUCCESSOR	          TRANSFORMATION_TYPE	        LEVEL
		CL_DEPOSIT_SERVICE	       LDG_DEPOSIT_SERVICE	      LANDING	                     3
		CL_DEPOSIT_TXN	           LDG_DEPOSIT_TXN	          LANDING	                     3
		LDG_DEPOSIT_SERVICE	       STG_ACCOUNT_SUMMARY	      AGGREGATION	                 2
		LDG_DEPOSIT_TXN	           STG_ACCOUNT_SUMMARY	      AGGREGATION	                 2
		STG_ACCOUNT_SUMMARY	       FCT_ACCOUNT_SUMMARY	      CALCULATION	                 1
		*/

		TestUtilityMethods.loadDIHelper();
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new BigDataEngine();
		coreEngine.setEngineId(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
		
		engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("Movie_RDBMS");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);

 		OWLERLineage owler = new OWLERLineage(coreEngine, coreEngine.getOWL());
 		
		owler.addLineageRelation("CL_DEPOSIT_SERVICE", "LDG_DEPOSIT_SERVICE");
		owler.addLineageProp("CL_DEPOSIT_SERVICE", "LDG_DEPOSIT_SERVICE","Landing","3");
		
		owler.addLineageRelation("CL_DEPOSIT_TXN", "LDG_DEPOSIT_TXN");
		owler.addLineageProp("CL_DEPOSIT_TXN", "LDG_DEPOSIT_TXN","Landing","3");
		
		owler.addLineageRelation("LDG_DEPOSIT_SERVICE", "STG_ACCOUNT_SUMMARY");
		owler.addLineageProp("LDG_DEPOSIT_SERVICE", "STG_ACCOUNT_SUMMARY","AGGREGATION","2");
		
		owler.addLineageRelation("LDG_DEPOSIT_TXN", "STG_ACCOUNT_SUMMARY");
		owler.addLineageProp("LDG_DEPOSIT_TXN", "STG_ACCOUNT_SUMMARY","AGGREGATION","2");
		
		owler.addLineageRelation("CL_DEPOSIT_SERVICE", "LDG_DEPOSIT_SERVICE");
		owler.addLineageProp("STG_ACCOUNT_SUMMARY", "FCT_ACCOUNT_SUMMARY","CALCULATION","1");
		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		owler.getOwlAsString();
		
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		String query = "SELECT ?SOURCE ?REL ?TARGET ?PROP ?VALUE WHERE { "
				+ "{?SOURCE ?REL ?TARGET} "
				+ "{?REL <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Lineage>} "
				+ "OPTIONAL { {?REL ?PROP ?VALUE}"
					+ "{?PROP <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <LINEAGE:PROPERTY>} "
					+ "}"
				+ "}";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(((AbstractEngine) coreEngine).getBaseDataEngine(), query);
		while(wrapper.hasNext()) {
			System.out.println(Arrays.toString(wrapper.next().getRawValues()));
		}
	}
	
	public String getOwlAsString() {
		// this will both write the owl to a file and print it onto the console
		String owl = null;
		try {
			owl = engine.exportBaseEngAsString(true);
			System.out.println("OWL.. " + owl);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return owl;
	}
	///////////////////// END TESTING /////////////////////

	
	///////////////// GETTERS ///////////////////////
	
	/**
	 * Get the owl file path set in the owler
	 * @return
	 */
	public String getOwlPath() {
		return this.owlPath;
	}
	
	/*
	 * The getters exist for the conceptHash, relationHash, and propHash
	 * These are only used during RDF uploading
	 * RDF requires the meta data information to also be stored in the database
	 * along with the instance data
	 */
	
	public Hashtable<String, String> getLineageRelationtHash() {
		return lineageRelationHash;
	}
	public Hashtable<String, String> getLineagePropnHash() {
		return lineagePropHash;
	}
 
	///////////////// END GETTERS ///////////////////////

	///////////////// SETTERS ///////////////////////
	/*
	 * These methods are not actually used.. kinda here just in case we end up needing them
	 */
	public void setOwlPath(String owlPath) {
		this.owlPath = owlPath;
	}
	public void setLineageRelationtHash(Hashtable<String, String> lineageRelationHash) {
		this.lineageRelationHash = lineageRelationHash;
	}
	public void setLineagePropnHash(Hashtable<String, String> lineagePropHash) {
		this.lineagePropHash = lineagePropHash;
	}
 
	///////////////// END SETTERS ///////////////////////
}
