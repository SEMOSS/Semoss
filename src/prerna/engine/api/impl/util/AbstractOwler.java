//package prerna.engine.api.impl.util;
//
//import java.io.IOException;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.Hashtable;
//import java.util.List;
//import java.util.Properties;
//import java.util.Set;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.openrdf.model.vocabulary.RDF;
//import org.openrdf.model.vocabulary.RDFS;
//
//import com.hp.hpl.jena.vocabulary.OWL;
//
//import prerna.engine.api.IDatabaseEngine;
//import prerna.engine.api.IDatabaseEngine.ACTION_TYPE;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.engine.impl.rdf.RDFFileSesameEngine;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.util.Constants;
//import prerna.util.Utility;
//
//public abstract class AbstractOwler {
//
//	private static final Logger classLogger = LogManager.getLogger(AbstractOwler.class);
//	
//	// predefined URIs
//	public static final String SEMOSS_URI_PREFIX = "http://semoss.org/ontologies/";
//	public static final String DEFAULT_NODE_CLASS = "Concept";
//	public static final String DEFAULT_RELATION_CLASS = "Relation";
//	public static final String DEFAULT_PROP_CLASS = "Relation/Contains";
//	public static final String CONCEPTUAL_RELATION_NAME = "Conceptual";
//	public static final String PIXEL_RELATION_NAME = "Pixel";
//	public static final String ADDITIONAL_DATATYPE_NAME = "AdtlDataType";
//	
//	// since we keep making these URIs often
//	public static final String BASE_NODE_URI = SEMOSS_URI_PREFIX + DEFAULT_NODE_CLASS;
//	public static final String BASE_RELATION_URI = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;
//	public static final String BASE_PROPERTY_URI = SEMOSS_URI_PREFIX + DEFAULT_PROP_CLASS;
//	public static final String CONCEPTUAL_RELATION_URI = BASE_RELATION_URI + "/" + CONCEPTUAL_RELATION_NAME;
//	public static final String PIXEL_RELATION_URI = BASE_RELATION_URI + "/" + PIXEL_RELATION_NAME;
//	public static final String ADDITIONAL_DATATYPE_RELATION_URI = BASE_RELATION_URI + "/" + ADDITIONAL_DATATYPE_NAME;
//	
//	public static final String TIME_KEY = "ENGINE:TIME";
//	public static final String TIME_URL = "http://semoss.org/ontologies/Concept/TimeStamp";
//	private final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
//	
//	@Deprecated
//	public static final String LEGACY_PRIM_KEY_URI = BASE_RELATION_URI + "/" + "LEGACY_PRIM_KEY";
//	
//	// hashtable of concepts
//	protected Hashtable<String, String> conceptHash = new Hashtable<String, String>();
//	// hashtable of relationships
//	protected Hashtable<String, String> relationHash = new Hashtable<String, String>();
//	// hashtable of properties
//	protected Hashtable<String, String> propHash = new Hashtable<String, String>();
//	// set of conceptual names
//	protected Set<String> pixelNames = new HashSet<String>();
//	// need to know the database type due to differences in URIs when the
//	// database is RDF vs. RDBMS
//	protected IDatabaseEngine.DATABASE_TYPE type = null;
//	
//	protected IDatabaseEngine engine;
//	protected RDFFileSesameEngine owlEngine;
//	protected String owlPath;
//	
//	/**
//	 * Constructor for the class when we are creating a brand new OWL file
//	 * @param fileName				The location of the new OWL file
//	 * @param type					The type of the engine the OWL file is being created for
//	 * @throws Exception 
//	 */
//	public AbstractOwler(String engineId, String owlPath, IDatabaseEngine.DATABASE_TYPE type) throws Exception {
//		this.type = type;
//
//		this.owlEngine = new RDFFileSesameEngine();
//		this.owlEngine.open(new Properties());
//		this.owlEngine.setFilePath(owlPath);
//		this.owlEngine.setEngineId(engineId + "_" + Constants.OWL_ENGINE_SUFFIX);
//		this.owlPath = owlPath;
//		
//		String baseSubject = SEMOSS_URI_PREFIX + DEFAULT_NODE_CLASS ;
//		String baseRelation = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;
//		String predicate = RDF.TYPE.stringValue();
//
//		addToBaseEngine(baseSubject, predicate, RDFS.CLASS.stringValue());
//		addToBaseEngine(baseRelation, predicate, RDF.PROPERTY.stringValue());
//	}
//
//	/**
//	 * Constructor for the class when we are adding to an existing OWL file
//	 * @param existingEngine		The engine we are adding to
//	 */
//	public AbstractOwler(IDatabaseEngine existingEngine) {
//		this.engine = existingEngine;
//		this.type = existingEngine.getDatabaseType();
//		this.owlPath = existingEngine.getOwlFilePath();
//		this.owlEngine = existingEngine.getBaseDataEngine();
//	}
//
//	/**
//	 * Adding information into the base engine
//	 * Currently assumes we are only adding URIs (object is never a literal)
//	 * @param triple 			The triple to load into the engine and into baseDataHash
//	 */
//	public void addToBaseEngine(Object[] triple) {
//		String sub = (String) triple[0];
//		String pred = (String) triple[1];
//		// is this a URI or a literal?
//		boolean concept = Boolean.valueOf((boolean) triple[3]);
//
//		String cleanSub = Utility.cleanString(sub, false);
//		String cleanPred = Utility.cleanString(pred, false);
//		
//		Object objValue = triple[2];
//		// if it is a URI
//		// gotta clean up the value
//		if(concept) {
//			objValue = Utility.cleanString(objValue.toString(), false);
//		}
//		
//		owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{cleanSub, cleanPred, objValue, concept});
//	}
//	
//	/**
//	 * Adding information into the base engine
//	 * Currently assumes we are only adding URIs (object is never a literal)
//	 * @param triple 			The triple to load into the engine and into baseDataHash
//	 */
//	public void removeFromBaseEngine(Object[] triple) {
//		String sub = (String) triple[0];
//		String pred = (String) triple[1];
//		String obj = (String) triple[2];
//		boolean concept = Boolean.valueOf((boolean) triple[3]);
//
//		String cleanSub = Utility.cleanString(sub, false);
//		String cleanPred = Utility.cleanString(pred, false);
//
//		Object objValue = triple[2];
//		// if it is a URI
//		// gotta clean up the value
//		if(concept) {
//			objValue = Utility.cleanString(objValue.toString(), false);
//		}
//		
//		owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{cleanSub, cleanPred, objValue, concept});
//	}
//	
//	// set this as separate pieces as well
//	public void addToBaseEngine(String subject, String predicate, String object) {
//		addToBaseEngine(new Object[]{subject, predicate, object, true});
//	}
//	
//	public void addToBaseEngine(String subject, String predicate, Object object, boolean isUri) {
//		addToBaseEngine(new Object[]{subject, predicate, object, isUri});
//	}
//	
//	// set this as separate pieces as well
//	public void removeFromBaseEngine(String subject, String predicate, String object) {
//		removeFromBaseEngine(new Object[]{subject, predicate, object, true});
//	}
//
//	public void removeFromBaseEngine(String subject, String predicate, Object object, boolean isUri) {
//		removeFromBaseEngine(new Object[]{subject, predicate, object, isUri});
//	}
//	
//	/**
//	 * 
//	 * @throws IOException
//	 */
//	public void export() throws IOException {
//		export(true);
//	}
//	
//	/**
//	 * 
//	 * @throws IOException
//	 */
//	public void export(boolean addTimeStamp) throws IOException {
//		try {
//			//adding a time-stamp to the OWL file
//			if(addTimeStamp) {
//				deleteExisitngTimestamp();
//				Calendar cal = Calendar.getInstance();
//				String cleanObj = DATE_FORMATTER.format(cal.getTime());
//				this.owlEngine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{TIME_URL, TIME_KEY, cleanObj, false});
//			}
//			this.owlEngine.exportDB();
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//			throw new IOException("Error in writing OWL file");
//		}
//	}
//
//	private void deleteExisitngTimestamp() {
//		String getAllTimestampQuery = "SELECT DISTINCT ?time ?val WHERE { "
//				+ "BIND(<http://semoss.org/ontologies/Concept/TimeStamp> AS ?time)"
//				+ "{?time <" + TIME_KEY + "> ?val} "
//				+ "}";
//		
//		List<String> currTimes = new ArrayList<>();
//
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(owlEngine, getAllTimestampQuery);
//			while(wrapper.hasNext()) {
//				IHeadersDataRow row = wrapper.next();
//				Object[] rawRow = row.getRawValues();
//				Object[] cleanRow = row.getValues();
//				currTimes.add(rawRow[0] + "");
//				currTimes.add(cleanRow[1] + "");
//			}
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(wrapper != null) {
//				try {
//					wrapper.close();
//				} catch (IOException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//		
//		for(int delIndex = 0; delIndex < currTimes.size(); delIndex+=2) {
//			Object[] delTriples = new Object[4];
//			delTriples[0] = currTimes.get(delIndex);
//			delTriples[1] = TIME_KEY;
//			delTriples[2] = currTimes.get(delIndex+1);
//			delTriples[3] = false;
//			
//			this.owlEngine.doAction(ACTION_TYPE.REMOVE_STATEMENT, delTriples);
//		}
//	}
//
//	/**
//	 * Commits the triples added to the base engine
//	 */
//	public void commit() {
//		owlEngine.commit();
//	}
//	
//	/**
//	 * 
//	 * @return
//	 */
//	public RDFFileSesameEngine getBaseEng() {
//		return this.owlEngine;
//	}
//
//	/**
//	 * @throws IOException 
//	 * 
//	 */
//	public void closeOwl() throws IOException {
//		this.owlEngine.close();
//	}
//	
//	/////////////////// ADD LOGICAL NAMES AND DESCRIPTIONS INTO THE OWL /////////////////////////////////
//
//	/**
//	 * Add logical names to a physical uri
//	 * @param physicalUri
//	 * @param logicalNames
//	 */
//	public void addLogicalNames(String physicalUri, String... logicalNames) {
//		if(logicalNames != null) {
//			for(String lName : logicalNames) {
//				if(lName != null && !lName.isEmpty()) {
//					this.addToBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
//				}
//			}
//		}
//	}
//	
//	public void addLogicalNames(String physicalUri, Collection<String> logicalNames) {
//		if(logicalNames != null) {
//			for(String lName : logicalNames) {
//				if(lName != null && !lName.isEmpty()) {
//					this.addToBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
//				}
//			}
//		}
//	}
//	
//	/**
//	 * Remove logical names from a physical uri
//	 * @param physicalUri
//	 * @param logicalNames
//	 */
//	public void deleteLogicalNames(String physicalUri, String... logicalNames) {
//		if(logicalNames != null) {
//			for(String lName : logicalNames) {
//				if(lName != null && !lName.isEmpty()) {
//					this.removeFromBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
//				}
//			}
//		}
//	}
//	
//	/**
//	 * Remove logical names from a physical uri
//	 * @param physicalUri
//	 * @param logicalNames
//	 */
//	public void deleteLogicalNames(String physicalUri, Collection<String> logicalNames) {
//		if(logicalNames != null) {
//			for(String lName : logicalNames) {
//				if(lName != null && !lName.isEmpty()) {
//					this.removeFromBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
//				}
//			}
//		}
//	}
//	
//	/**
//	 * Add descriptions to a physical uri
//	 * @param physicalUri
//	 * @param description
//	 */
//	public void addDescription(String physicalUri, String description) {
//		if(description != null && !description.trim().isEmpty()) {
//			description = description.replaceAll("[^\\p{ASCII}]", "");
//			this.addToBaseEngine(new Object[]{physicalUri, RDFS.COMMENT.toString(), description, false});
//		}
//	}
//	
//	/**
//	 * Remove descriptions to a physical uri
//	 * @param physicalUri
//	 * @param description
//	 */
//	public void deleteDescription(String physicalUri, String description) {
//		if(description != null && !description.trim().isEmpty()) {
//			description = description.replaceAll("[^\\p{ASCII}]", "");
//			this.removeFromBaseEngine(new Object[]{physicalUri, RDFS.COMMENT.toString(), description, false});
//		}
//	}
//	
//	/////////////////// END ADDING LOGICAL NAMES INTO THE OWL /////////////////////////////////
//
//	
//	/////////////////// ADDITIONAL METHODS TO INSERT INTO THE OWL /////////////////////////////////
//	
//	/**
//	 * Store the custom base URI used to create instance URIs within the OWL
//	 * E.g. of usage is current RDF MHS databases, which use "http://health.mil/ontologies" as the custom base URI
//	 * @param customBaseURI				The customBaseURI to store
//	 */
//	public void addCustomBaseURI(String customBaseURI) {
//		this.addToBaseEngine("SEMOSS:ENGINE_METADATA", "CONTAINS:BASE_URI", customBaseURI+"/"+DEFAULT_NODE_CLASS+"/");
//	}
//	
//	/////////////////// END ADDITIONAL METHODS TO INSERT INTO THE OWL /////////////////////////////////
//	
//	
//	///////////////// GETTERS ///////////////////////
//	
//	/*
//	 * The getters exist for the conceptHash, relationHash, and propHash
//	 * These are only used during RDF uploading
//	 * RDF requires the meta data information to also be stored in the database
//	 * along with the instance data
//	 */
//	
//	public Hashtable<String, String> getConceptHash() {
//		return conceptHash;
//	}
//	
//	public Hashtable<String, String> getRelationHash() {
//		return relationHash;
//	}
//	
//	public Hashtable<String, String> getPropHash() {
//		return propHash;
//	}
//	
//	public Set<String> getPixelNames() {
//		return pixelNames;
//	}
//	
//	public RDFFileSesameEngine getOwlEngine() {
//		return this.owlEngine;
//	}
//	
//	public String getOwlPath() {
//		return this.owlPath;
//	}
//	
//	///////////////// END GETTERS ///////////////////////
//
//	///////////////// SETTERS ///////////////////////
//	
//	public void setConceptHash(Hashtable<String, String> conceptHash) {
//		this.conceptHash = conceptHash;
//	}
//	
//	public void setRelationHash(Hashtable<String, String> relationHash) {
//		this.relationHash = relationHash;
//	}
//	
//	public void setPropHash(Hashtable<String, String> propHash) {
//		this.propHash = propHash;
//	}
//	
//	///////////////// END SETTERS ///////////////////////
//
//}
