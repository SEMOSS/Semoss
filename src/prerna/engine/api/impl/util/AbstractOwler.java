package prerna.engine.api.impl.util;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.hp.hpl.jena.vocabulary.OWL;

import prerna.engine.api.IDatabaseEngine;
import prerna.poi.main.BaseDatabaseCreator;

public abstract class AbstractOwler {

	// predefined URIs
	public static final String SEMOSS_URI_PREFIX = "http://semoss.org/ontologies/";
	public static final String DEFAULT_NODE_CLASS = "Concept";
	public static final String DEFAULT_RELATION_CLASS = "Relation";
	public static final String DEFAULT_PROP_CLASS = "Relation/Contains";
	public static final String CONCEPTUAL_RELATION_NAME = "Conceptual";
	public static final String PIXEL_RELATION_NAME = "Pixel";
	public static final String ADDITIONAL_DATATYPE_NAME = "AdtlDataType";
	
	// since we keep making these URIs often
	public static final String BASE_NODE_URI = SEMOSS_URI_PREFIX + DEFAULT_NODE_CLASS;
	public static final String BASE_RELATION_URI = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;
	public static final String BASE_PROPERTY_URI = SEMOSS_URI_PREFIX + DEFAULT_PROP_CLASS;
	public static final String CONCEPTUAL_RELATION_URI = BASE_RELATION_URI + "/" + CONCEPTUAL_RELATION_NAME;
	public static final String PIXEL_RELATION_URI = BASE_RELATION_URI + "/" + PIXEL_RELATION_NAME;
	public static final String ADDITIONAL_DATATYPE_RELATION_URI = BASE_RELATION_URI + "/" + ADDITIONAL_DATATYPE_NAME;
	
	@Deprecated
	public static final String LEGACY_PRIM_KEY_URI = BASE_RELATION_URI + "/" + "LEGACY_PRIM_KEY";
	
	// hashtable of concepts
	protected Hashtable<String, String> conceptHash = new Hashtable<String, String>();
	// hashtable of relationships
	protected Hashtable<String, String> relationHash = new Hashtable<String, String>();
	// hashtable of properties
	protected Hashtable<String, String> propHash = new Hashtable<String, String>();
	// set of conceptual names
	protected Set<String> pixelNames = new HashSet<String>();
	// need to know the database type due to differences in URIs when the
	// database is RDF vs. RDBMS
	protected IDatabaseEngine.DATABASE_TYPE type = null;
	
	
	// the engine here is a wrapper around a RDFFileSesameEngine which helps with adding the URIs into the engine
	protected BaseDatabaseCreator engine = null;
	// file name for the location of the OWL file to write to
	protected String owlPath = null;

	/**
	 * Constructor for the class when we are creating a brand new OWL file
	 * @param fileName				The location of the new OWL file
	 * @param type					The type of the engine the OWL file is being created for
	 * @throws Exception 
	 */
	public AbstractOwler(String engineId, String owlPath, IDatabaseEngine.DATABASE_TYPE type) throws Exception {
		this.owlPath = owlPath;
		this.type = type;

		engine = new BaseDatabaseCreator(engineId, owlPath);
		String baseSubject = SEMOSS_URI_PREFIX + DEFAULT_NODE_CLASS ;
		String baseRelation = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;

		String predicate = RDF.TYPE.stringValue();

		engine.addToBaseEngine(baseSubject, predicate, RDFS.CLASS.stringValue());
		engine.addToBaseEngine(baseRelation, predicate, RDF.PROPERTY.stringValue());
	}

	/**
	 * Constructor for the class when we are adding to an existing OWL file
	 * @param existingEngine		The engine we are adding to
	 */
	public AbstractOwler(IDatabaseEngine existingEngine) {
		this.owlPath = existingEngine.getOWL();
		this.type = existingEngine.getDatabaseType();
		engine = new BaseDatabaseCreator(existingEngine, owlPath);
	}

	/**
	 * Closes the connection to the RDFFileSesameEngine supported by the OWL
	 * @throws IOException 
	 */
	public void closeOwl() throws IOException {
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
	
	/////////////////// ADD LOGICAL NAMES AND DESCRIPTIONS INTO THE OWL /////////////////////////////////

	/**
	 * Add logical names to a physical uri
	 * @param physicalUri
	 * @param logicalNames
	 */
	public void addLogicalNames(String physicalUri, String... logicalNames) {
		if(logicalNames != null) {
			for(String lName : logicalNames) {
				if(lName != null && !lName.isEmpty()) {
					this.engine.addToBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
				}
			}
		}
	}
	
	public void addLogicalNames(String physicalUri, Collection<String> logicalNames) {
		if(logicalNames != null) {
			for(String lName : logicalNames) {
				if(lName != null && !lName.isEmpty()) {
					this.engine.addToBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
				}
			}
		}
	}
	
	/**
	 * Remove logical names from a physical uri
	 * @param physicalUri
	 * @param logicalNames
	 */
	public void deleteLogicalNames(String physicalUri, String... logicalNames) {
		if(logicalNames != null) {
			for(String lName : logicalNames) {
				if(lName != null && !lName.isEmpty()) {
					this.engine.removeFromBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
				}
			}
		}
	}
	
	/**
	 * Remove logical names from a physical uri
	 * @param physicalUri
	 * @param logicalNames
	 */
	public void deleteLogicalNames(String physicalUri, Collection<String> logicalNames) {
		if(logicalNames != null) {
			for(String lName : logicalNames) {
				if(lName != null && !lName.isEmpty()) {
					this.engine.removeFromBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
				}
			}
		}
	}
	
	/**
	 * Add descriptions to a physical uri
	 * @param physicalUri
	 * @param description
	 */
	public void addDescription(String physicalUri, String description) {
		if(description != null && !description.trim().isEmpty()) {
			description = description.replaceAll("[^\\p{ASCII}]", "");
			this.engine.addToBaseEngine(new Object[]{physicalUri, RDFS.COMMENT.toString(), description, false});
		}
	}
	
	/**
	 * Remove descriptions to a physical uri
	 * @param physicalUri
	 * @param description
	 */
	public void deleteDescription(String physicalUri, String description) {
		if(description != null && !description.trim().isEmpty()) {
			description = description.replaceAll("[^\\p{ASCII}]", "");
			this.engine.removeFromBaseEngine(new Object[]{physicalUri, RDFS.COMMENT.toString(), description, false});
		}
	}
	
	/////////////////// END ADDING LOGICAL NAMES INTO THE OWL /////////////////////////////////

	
	/////////////////// ADDITIONAL METHODS TO INSERT INTO THE OWL /////////////////////////////////
	
	/**
	 * Store the custom base URI used to create instance URIs within the OWL
	 * E.g. of usage is current RDF MHS databases, which use "http://health.mil/ontologies" as the custom base URI
	 * @param customBaseURI				The customBaseURI to store
	 */
	public void addCustomBaseURI(String customBaseURI) {
		engine.addToBaseEngine("SEMOSS:ENGINE_METADATA", "CONTAINS:BASE_URI", customBaseURI+"/"+DEFAULT_NODE_CLASS+"/");
	}
	
	/////////////////// END ADDITIONAL METHODS TO INSERT INTO THE OWL /////////////////////////////////
	
	
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
	
	public Hashtable<String, String> getConceptHash() {
		return conceptHash;
	}
	
	public Hashtable<String, String> getRelationHash() {
		return relationHash;
	}
	
	public Hashtable<String, String> getPropHash() {
		return propHash;
	}
	
	public Set<String> getPixelNames() {
		return pixelNames;
	}
	
	///////////////// END GETTERS ///////////////////////

	///////////////// SETTERS ///////////////////////
	
	public void setOwlPath(String owlPath) {
		this.owlPath = owlPath;
	}
	
	public void setConceptHash(Hashtable<String, String> conceptHash) {
		this.conceptHash = conceptHash;
	}
	
	public void setRelationHash(Hashtable<String, String> relationHash) {
		this.relationHash = relationHash;
	}
	
	public void setPropHash(Hashtable<String, String> propHash) {
		this.propHash = propHash;
	}
	
	///////////////// END SETTERS ///////////////////////

}
