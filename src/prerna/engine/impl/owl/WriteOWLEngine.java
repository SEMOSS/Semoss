package prerna.engine.impl.owl;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.hp.hpl.jena.vocabulary.OWL;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.ACTION_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.impl.util.MetadataUtility;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.upload.UploadUtilities;

public class WriteOWLEngine extends AbstractOWLEngine implements Closeable {

	private static final Logger classLogger = LogManager.getLogger(WriteOWLEngine.class);

	private final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");

	// hashtable of concepts
	protected Hashtable<String, String> conceptHash = new Hashtable<String, String>();
	// hashtable of relationships
	protected Hashtable<String, String> relationHash = new Hashtable<String, String>();
	// hashtable of properties
	protected Hashtable<String, String> propHash = new Hashtable<String, String>();
	// set of conceptual names
	protected Set<String> pixelNames = new HashSet<String>();
	
	private final Semaphore writeSemaphore;
	private IDatabaseEngine.DATABASE_TYPE dbType = IDatabaseEngine.DATABASE_TYPE.RDBMS;

	public WriteOWLEngine(Semaphore writeSemaphore, 
			RDFFileSesameEngine baseDataEngine, 
			IDatabaseEngine.DATABASE_TYPE dbType, 
			String engineId, 
			String engineName) {
		super(baseDataEngine, engineId, engineName);
		this.dbType = dbType;

		if(writeSemaphore == null) {
			throw new NullPointerException("Cannot have a null semaphore");
		}
		
		loadDatabaseValues();
		
		this.writeSemaphore = writeSemaphore;
	}
	
	@Override
	public void close() throws IOException {
		this.writeSemaphore.release();
	}
	
	protected void loadDatabaseValues() {
		Hashtable<String, String> conceptHash = new Hashtable<>();
		Hashtable<String, String> propHash = new Hashtable<>();
		Hashtable<String, String> relationHash = new Hashtable<>();

		boolean isRdbms = (this.dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS
				|| this.dbType == IDatabaseEngine.DATABASE_TYPE.IMPALA);

		List<String> concepts = this.getPhysicalConcepts();
		for (String cUri : concepts) {
			String tableName = Utility.getInstanceName(cUri);
			String cKey = tableName;
			if (isRdbms) {
				cKey = Utility.getClassName(cUri) + cKey;
			}
			// add to concept hash
			conceptHash.put(cKey, cUri);

			// add all the props as well
			List<String> props = this.getPropertyUris4PhysicalUri(cUri);
			for (String p : props) {
				String propName = null;
				if (isRdbms) {
					propName = Utility.getClassName(p);
				} else {
					propName = Utility.getInstanceName(p);
				}

				propHash.put(tableName + "%" + propName, p);
			}
		}

		List<String[]> rels = this.getPhysicalRelationships();
		for (String[] r : rels) {
			String startT = null;
			String startC = null;
			String endT = null;
			String endC = null;
			String pred = null;

			startT = Utility.getInstanceName(r[0]);
			endT = Utility.getInstanceName(r[1]);
			pred = Utility.getInstanceName(r[2]);

			if (isRdbms) {
				startC = Utility.getClassName(r[0]);
				endC = Utility.getClassName(r[1]);
			}

			relationHash.put(startT + startC + endT + endC + pred, r[2]);
		}

		setConceptHash(conceptHash);
		setPropHash(propHash);
		setRelationHash(relationHash);
	}
	
	/**
	 * @throws Exception 
	 * 
	 */
	public void createEmptyOWLFile() throws Exception {
		this.baseDataEngine.close();
		this.baseDataEngine.deleteFile();
		UploadUtilities.generateOwlFile(this.baseDataEngine.getFilePath());
		this.baseDataEngine.reloadFile();
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	public void reloadOWLFile() throws Exception {
		this.baseDataEngine.reloadFile();
	}
	
	/*
	 * Adding into the OWL
	 */

	/////////////////// ADDING CONCEPTS INTO THE OWL ////////////////// 

	/**
	 * Add a concept to the OWL If RDF : a concept has a data type (String) If RDBMS
	 * : this will represent a table and not have a datatype
	 * 
	 * @param tableName
	 * @param dataType
	 * @param conceptual
	 * @return
	 */
	public String addConcept(String tableName, String dataType, String conceptual) {
		// since RDF uses this multiple times, don't create it each time and just store
		// it in a hash to send back
		if (!conceptHash.containsKey(tableName)) {
			// here is the logic to create the physical uri for the concept
			// the base URI for the concept will be the baseNodeURI
			String subject = BASE_NODE_URI + "/" + tableName;

			// now lets start to add the triples
			// lets add the triples pertaining to those numbered above

			// 1) adding the physical URI concept as a subClassOf the baseNodeURI
			this.addToBaseEngine(subject, RDFS.SUBCLASSOF.stringValue(), BASE_NODE_URI);

			// 2) now lets add the dataType of the concept
			// this will only apply if it is RDF
			if (dataType != null) {
				String typeObject = "TYPE:" + dataType;
				this.addToBaseEngine(subject, RDFS.CLASS.stringValue(), typeObject);
			}
			if (MetadataUtility.ignoreConceptData(this.dbType)) {
				// add an ignore data tag so we can easily query
				this.addToBaseEngine(subject, RDFS.DOMAIN.toString(), "noData", false);
			}

			// 3) now lets add the physical URI to the pixel name URI
			String pixelName = Utility.cleanVariableString(tableName);
			pixelNames.add(pixelName);
			String pixelUri = BASE_NODE_URI + "/" + pixelName;
			this.addToBaseEngine(subject, PIXEL_RELATION_URI, pixelUri);

			// 4) let us add the original table name as the conceptual name
			if (conceptual == null) {
				conceptual = tableName;
			}
			this.addToBaseEngine(subject, CONCEPTUAL_RELATION_URI, conceptual, false);

			// store it in the hash for future use
			// NOTE : The hash contains the physical URI
			conceptHash.put(tableName, subject);
		}
		return conceptHash.get(tableName);
	}

	public String addConcept(String tableName, String dataType) {
		return addConcept(tableName, dataType, null);
	}

	public String addConcept(String concept) {
		return addConcept(concept, "STRING", null);
	}

	////////////////////////////////// END ADDING CONCEPTS INTO THE OWL //////////////////////////////////

	////////////////////////////////// ADDING RELATIONSHIP INTO THE OWL //////////////////////////////////

	/**
	 * Add a relationship between two concepts In RDBMS : the predicate must be
	 * fromTable.fromColumn.toTable.toColumn
	 * 
	 * @param fromTable
	 * @param toTable
	 * @param predicate
	 * @return
	 */
	public String addRelation(String fromTable, String toTable, String predicate) {
		// since RDF uses this multiple times, don't create it each time and just store
		// it in a hash to send back
		if (!relationHash.containsKey(fromTable + toTable + predicate)) {

			// need to make sure both the fromConcept and the toConcept are already defined
			// as concepts
			// TODO: this works for RDBMS even though it only takes in the concept names
			// because we usually perform
			// the addConcept call before... this is really just intended to retrieve the
			String fromConceptURI = addConcept(fromTable, null, null);
			String toConceptURI = addConcept(toTable, null, null);

			// create the base relationship uri
			String baseRelationURI = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;
			String predicateSubject = baseRelationURI + "/" + predicate;

			// now lets start to add the triples
			// lets add the triples pertaining to those numbered above

			// 1) now add the physical relationship URI
			this.addToBaseEngine(predicateSubject, RDFS.SUBPROPERTYOF.stringValue(), baseRelationURI);

			// 2) now add the relationship between the two nodes
			this.addToBaseEngine(fromConceptURI, predicateSubject, toConceptURI);

			// lastly, store it in the hash for future use
			relationHash.put(fromTable + toTable + predicate, predicateSubject);
		}
		return relationHash.get(fromTable + toTable + predicate);
	}

	////////////////////////////////// END ADDING RELATIONSHIP INTO THE OWL //////////////////////////////////

	////////////////////////////////// ADDING PROPERTIES TO CONCEPTS IN THE OWL //////////////////////////////////

	/**
	 * Add a property to a given concept
	 * 
	 * @param tableName
	 * @param propertyCol
	 * @param dataType
	 * @param adtlDataType
	 * @param conceptual
	 * @return
	 */
	public String addProp(String tableName, String propertyCol, String dataType, String adtlDataType, String conceptual) {
		if (!propHash.containsKey(tableName + "%" + propertyCol)) {
			String conceptURI = addConcept(tableName, null, null);

			// create the property URI
			String property = null;
			if (this.dbType == IDatabaseEngine.DATABASE_TYPE.SESAME) {
				// THIS IS BECAUSE OF LEGACY QUERIES!!!
				property = BASE_PROPERTY_URI + "/" + propertyCol;
			} else {
				property = BASE_PROPERTY_URI + "/" + propertyCol + "/" + tableName;
			}

			// now lets start to add the triples
			// lets add the triples pertaining to those numbered above

			// 1) adding the property as type of base property URI
			this.addToBaseEngine(property, RDF.TYPE.stringValue(), BASE_PROPERTY_URI);

			// 2) adding the property to the concept
			this.addToBaseEngine(conceptURI, OWL.DatatypeProperty.toString(), property);

			// 3) adding the property data type
			String typeObject = "TYPE:" + dataType;
			this.addToBaseEngine(property, RDFS.CLASS.stringValue(), typeObject);

			// 4) adding the property additional data type, if available
			if (adtlDataType != null && !adtlDataType.isEmpty()) {
				String adtlTypeObject = "ADTLTYPE:" + adtlDataType;
				this.addToBaseEngine(property, ADDITIONAL_DATATYPE_RELATION_URI, adtlTypeObject, false);
			}

			// 5) now lets add the physical URI to the pixel name URI
			String pixelName = Utility.cleanVariableString(propertyCol);
			String pixelFullName = pixelName + "/" + Utility.cleanVariableString(tableName);
			String pixelUri = BASE_PROPERTY_URI + "/" + pixelFullName;
			this.addToBaseEngine(property, PIXEL_RELATION_URI, pixelUri);

			// 5) let us add the original table name as the conceptual name
			if (conceptual == null) {
				conceptual = propertyCol;
			}
			this.addToBaseEngine(property, CONCEPTUAL_RELATION_URI, conceptual, false);

			// lastly, store it in the hash for future use
			// NOTE : The hash contains the physical URI
			propHash.put(tableName + "%" + propertyCol, property);
		}

		return propHash.get(tableName + "%" + propertyCol);
	}

	/**
	 * This method will add a property onto a concept in the OWL file There are some
	 * differences based on how the information is used based on if it is a RDF
	 * engine or a RDBMS engine
	 * 
	 * @param tableName    For RDF: This is the name of the concept For RDBMS: This
	 *                     is the name of the table where the concept exists. If the
	 *                     concept doesn't exist, it is assumed the column name of
	 *                     the concept is the same as the table name
	 * @param propertyCol  This will be the name of the property
	 * @param dataType     The dataType for the property
	 * @param adtlDataType Additional data type for the property
	 * @return Returns the physical URI for the node
	 */
	public String addProp(String tableName, String propertyCol, String dataType, String adtlDataType) {
		return addProp(tableName, propertyCol, dataType, adtlDataType, null);
	}

	public String addProp(String tableName, String propertyCol, String dataType) {
		return addProp(tableName, propertyCol, dataType, null, null);
	}

	/**
	 * This method will calculate the unique values in each column/property and add
	 * it to the owl file.
	 * 
	 * @param queryEngine
	 */
	public void addUniqueCounts(IDatabaseEngine queryEngine) {
		String uniqueCountProp = SEMOSS_URI_PREFIX + DEFAULT_PROP_CLASS + "/UNIQUE";

		List<String> pixelConcepts = queryEngine.getPixelConcepts();
		for (String pixelConcept : pixelConcepts) {
			List<String> pSelectors = queryEngine.getPixelSelectors(pixelConcept);
			for (String selectorPixel : pSelectors) {
				SelectQueryStruct qs = new SelectQueryStruct();
				QueryFunctionSelector newSelector = new QueryFunctionSelector();
				newSelector.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
				newSelector.setDistinct(true);
				QueryColumnSelector innerSelector = new QueryColumnSelector(selectorPixel);
				newSelector.addInnerSelector(innerSelector);
				qs.addSelector(newSelector);
				qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE);

				IRawSelectWrapper it = null;
				try {
					it = WrapperManager.getInstance().getRawWrapper(queryEngine, qs);
					if (!it.hasNext()) {
						continue;
					}
					long uniqueRows = ((Number) it.next().getValues()[0]).longValue();
					String propertyPhysicalUri = queryEngine.getPhysicalUriFromPixelSelector(selectorPixel);
					this.addToBaseEngine(propertyPhysicalUri, uniqueCountProp, uniqueRows, false);
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if(it != null) {
						try {
							it.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}
		}

		this.commit();
		try {
			this.export(false);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	////////////////////////////////// END ADDING PROPERTIES TO CONCEPTS INTO THE OWL //////////////////////////////////


	public void addLegacyPrimKey(String tableName, String columnName) {
		String physicalUri = conceptHash.get(tableName);
		if (physicalUri == null) {
			physicalUri = addConcept(tableName, null, null);
		}
		this.addToBaseEngine(physicalUri, LEGACY_PRIM_KEY_URI, columnName, false);
	}

	////////////////////////////////// ADDITIONAL METHODS TO INSERT INTO THE OWL //////////////////////////////////

	/**
	 * Have one class a subclass of another class
	 * This code is really intended for RDF databases... 
	 * not sure what use it will have to utilize this within an RDBMS
	 * 
	 * @param childType  The child concept node
	 * @param parentType The parent concept node
	 */
	public void addSubclass(String childType, String parentType) {
		String childURI = addConcept(childType);
		String parentURI = addConcept(parentType);
		this.addToBaseEngine(childURI, RDFS.SUBCLASSOF.stringValue(), parentURI);
	}

	////////////////////////////////// END ADDITIONAL METHODS TO INSERT INTO THE OWL //////////////////////////////////

	/*
	 * REMOVING FROM THE OWL
	 */

	////////////////////////////////// REMOVING CONCEPTS FROM THE OWL //////////////////////////////////
	/**
	 * Remove a concept from the OWL If RDF : a concept has a data type (String) If
	 * RDBMS : this will represent a table and not have a datatype
	 * 
	 * @param appId      id of app
	 * @param tableName  name of concept/table
	 * @param dataType   data type of column values
	 * @param conceptual
	 * @return
	 */
	public NounMetadata removeConcept(String tableName) {
		// since RDF uses this multiple times, don't create it each time and just store
		// it in a hash to send back
		if (!conceptHash.containsKey(tableName)) {
			// create the physical uri for the concept
			// the base URI for the concept will be the baseNodeURI
			String conceptPhysical = this.getPhysicalUriFromPixelSelector(tableName);
			List<String> properties = this.getPropertyUris4PhysicalUri(conceptPhysical);
			StringBuilder bindings = new StringBuilder();
			for (String prop : properties) {
				bindings.append("(<").append(prop).append(">)");
			}

			// remove relationships to node
			List<String[]> fkRelationships = getPhysicalRelationships(this.baseDataEngine);
			classLogger.info("Removing relationships for concept='"+tableName+"'");
			for (String[] relations: fkRelationships) {
				String instanceName = Utility.getInstanceName(relations[2]);
				String[] tablesAndPrimaryKeys = instanceName.split("\\.");

				for (int i=0; i < tablesAndPrimaryKeys.length; i+=2) {
					String key = tablesAndPrimaryKeys[i];

					if (tableName.equalsIgnoreCase(key)) {
						this.baseDataEngine.doAction(ACTION_TYPE.REMOVE_STATEMENT, new Object[] { relations[0], relations[2], relations[1], true });
						this.baseDataEngine.doAction(ACTION_TYPE.REMOVE_STATEMENT, new Object[] { relations[2], RDFS.SUBPROPERTYOF.toString(), "http://semoss.org/ontologies/Relation", true });
					}
				}
			}

			if (bindings.length() > 0) {
				classLogger.info("Removing downstream props for concept='"+tableName+"'");
				// get everything downstream of the props
				{
					String query = "select ?s ?p ?o where { {?s ?p ?o} } bindings ?s {" + bindings.toString() + "}";

					IRawSelectWrapper it = null;
					try {
						it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, query);
						while (it.hasNext()) {
							IHeadersDataRow headerRows = it.next();
							executeRemoveQuery(headerRows, this.baseDataEngine);
						}
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						if(it != null) {
							try {
								it.close();
							} catch (IOException e) {
								classLogger.error(Constants.STACKTRACE, e);
							}
						}
					}
				}

				classLogger.info("Removing upstream props for concept='"+tableName+"'");
				// repeat for upstream of prop
				{
					String query = "select ?s ?p ?o where { {?s ?p ?o} } bindings ?o {"	+ bindings.toString() + "}";

					IRawSelectWrapper it = null;
					try {
						it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, query);
						while (it.hasNext()) {
							IHeadersDataRow headerRows = it.next();
							executeRemoveQuery(headerRows, this.baseDataEngine);
						}
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						if(it != null) {
							try {
								it.close();
							} catch (IOException e) {
								classLogger.error(Constants.STACKTRACE, e);
							}
						}
					}
				}
			}

			boolean hasTriple = false;

			classLogger.info("Removing downstream triples for concept='"+tableName+"'");
			// now repeat for the node itself
			// remove everything downstream of the node
			{
				String query = "select ?s ?p ?o where { bind(<" + conceptPhysical + "> as ?s) {?s ?p ?o} }";

				IRawSelectWrapper it = null;
				try {
					it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, query);
					while (it.hasNext()) {
						hasTriple = true;
						IHeadersDataRow headerRows = it.next();
						executeRemoveQuery(headerRows, this.baseDataEngine);
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if(it != null) {
						try {
							it.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}

			classLogger.info("Removing upstream triples for concept='"+tableName+"'");
			// repeat for upstream of the node
			{
				String query = "select ?s ?p ?o where { bind(<" + conceptPhysical + "> as ?o) {?s ?p ?o} }";

				IRawSelectWrapper it = null;
				try {
					it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, query);
					while (it.hasNext()) {
						hasTriple = true;
						IHeadersDataRow headerRows = it.next();
						executeRemoveQuery(headerRows, this.baseDataEngine);
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if(it != null) {
						try {
							it.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}

			if (!hasTriple) {
				throw new IllegalArgumentException("Cannot find concept in existing metadata to remove");
			}
		}
		
		// remove from hash
		conceptHash.remove(tableName);
		
		Iterator<String> propIterator = this.propHash.keySet().iterator();
		while(propIterator.hasNext()) {
			String thisProp = propIterator.next();
			if(thisProp.startsWith(tableName + "%")) {
				propIterator.remove();
			}
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully removed concept and all its dependencies",
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

	////////////////////////////////// END REMOVING CONCEPTS FROM THE OWL //////////////////////////////////


	////////////////////////////////// REMOVING RELATIONSHIPS FROM THE OWL //////////////////////////////////

	/**
	 * Remove an added predicate joining two tables together
	 * @param fromTable
	 * @param toTable
	 * @param predicate
	 */
	public void removeRelation(String fromTable, String toTable, String predicate) {
		String fromConceptURI = addConcept(fromTable, null, null);
		String toConceptURI = addConcept(toTable, null, null);

		// create the base relationship uri
		String baseRelationURI = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;
		String predicateSubject = baseRelationURI + "/" + predicate;

		// now lets start to add the triples
		// lets add the triples pertaining to those numbered above

		// 1) now add the physical relationship URI
		this.removeFromBaseEngine(predicateSubject, RDFS.SUBPROPERTYOF.stringValue(), baseRelationURI);

		// 2) now add the relationship between the two nodes
		this.removeFromBaseEngine(fromConceptURI, predicateSubject, toConceptURI);

		// lastly, store it in the hash for future use
		relationHash.remove(fromTable + toTable + predicate);
	}

	////////////////////////////////// END REMOVING RELATIONSHIPS FROM THE OWL //////////////////////////////////


	////////////////////////////////// REMOVING PROPERTIES FROM THE OWL //////////////////////////////////

	/**
	 * This method will remove a property from a concept in the OWL file There are some
	 * differences based on how the information is used based on if it is a RDF
	 * engine or a RDBMS engine
	 * 
	 * @param tableName
	 * @param propertyCol
	 * @return
	 * @return
	 */
	public NounMetadata removeProp(String tableName, String propertyCol) {
		// create the property URI
		String property = null;
		if (this.dbType == IDatabaseEngine.DATABASE_TYPE.SESAME) {
			// THIS IS BECAUSE OF LEGACY QUERIES!!!
			property = BASE_PROPERTY_URI + "/" + propertyCol;
		} else {
			property = BASE_PROPERTY_URI + "/" + propertyCol + "/" + tableName;
		}

		{
			// remove everything downstream of the property
			String downstreamQuery = "select ?s ?p ?o where { bind(<" + property + "> as ?s) " + "{?s ?p ?o} }";
			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, downstreamQuery);
				while (it.hasNext()) {
					IHeadersDataRow headerRows = it.next();
					executeRemoveQuery(headerRows, this.baseDataEngine);
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		{
			// repeat for upstream of the property
			String upstreamQuery = "select ?s ?p ?o where { bind(<" + property + "> as ?o) {?s ?p ?o} }";
			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, upstreamQuery);
				while (it.hasNext()) {
					IHeadersDataRow headerRows = it.next();
					executeRemoveQuery(headerRows, this.baseDataEngine);
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// remove from hash
		this.propHash.remove(tableName + "%" + propertyCol);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully removed property", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////
	
	// rename things
	
	/**
	 * Rename an old concept name to a new name
	 * @param appId
	 * @param oldConceptName
	 * @param newConceptName
	 * @return
	 */
	public NounMetadata renameConcept(String oldConceptName, String newConceptName, String newConceptualName) {
		// we need to take the table name and make the URL
		String newConceptPhysicalUri = BASE_NODE_URI + "/" + newConceptName;

		// then we need to take the properties of the table and store
		Map<String, String> newProperties = new HashMap<String, String>();
		Map<String, String> newRelations = new HashMap<String, String>();

		List<String> properties = null;
		
		// then everything downstream needs to be edited
		// then everything upstream needs to be edited
		List<Object[]> newTriplesToAdd = new ArrayList<>();
		List<Object[]> oldTriplesToDelete = new ArrayList<>();

		// then we need to change the property name as well to point to the new table name
		
		String oldConceptPhysical = this.getPhysicalUriFromPixelSelector(oldConceptName);
		properties = this.getPropertyUris4PhysicalUri(oldConceptPhysical);
		StringBuilder bindings = new StringBuilder();
		for (String oldProp : properties) {
			bindings.append("(<").append(oldProp).append(">)");

			// store new prop with new table name
			int index = oldProp.lastIndexOf("/");
			String newProp = oldProp.substring(0, index) + "/" + newConceptName;
			newProperties.put(oldProp, newProp);
		}

		// remove relationships to node
		List<String[]> fkRelationships = getPhysicalRelationships(this.baseDataEngine);
		String baseRelationURI = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;

		for (String[] relations : fkRelationships) {
			// track if change needs to be made
			boolean editRelation = false;
			String start = relations[0];
			String end = relations[1];
			String relURI = relations[2];
			String relationName = Utility.getInstanceName(relURI); // this is either a.b.c.d or x.a.b.y.c.d
			String[] tablesAndPrimaryKeys = relationName.split("\\.");
				
			String newStart = relations[0];
			String newEnd = relations[1];
			if (start.equals(oldConceptPhysical)) {
				editRelation = true;
				// need to change the start table
				newStart = newConceptPhysicalUri;

				// need to change the a in relationName
				if (tablesAndPrimaryKeys.length == 4) {
					// a.b.c.d
					tablesAndPrimaryKeys[0] = newConceptName;
				} else if (tablesAndPrimaryKeys.length == 6) {
					// this has the schema
					// x.a.b.y.c.d
					tablesAndPrimaryKeys[1] = newConceptName;
				}

			} else if (end.equals(oldConceptPhysical)) {
				editRelation = true;
				// need to change the end table
				newEnd = newConceptPhysicalUri;

				// need to change the c in relationName
				if (tablesAndPrimaryKeys.length == 4) {
					// a.b.c.d
					tablesAndPrimaryKeys[2] = newConceptName;
				} else if (tablesAndPrimaryKeys.length == 6) {
					// this has the schema
					// x.a.b.y.c.d
					tablesAndPrimaryKeys[4] = newConceptName;
				}
			}
			if (editRelation) {
				// create relationship name a.b.c.d or x.a.b.y.c.d
				String newRelName = String.join(".", tablesAndPrimaryKeys);
				String newRelationURI = baseRelationURI + "/" + newRelName;
				newRelations.put(relURI, newRelationURI);

				// store old relationship info
				oldTriplesToDelete.add(new Object[] { relations[0], relations[2], relations[1], true });
				oldTriplesToDelete.add(new Object[] { relations[2], RDFS.SUBPROPERTYOF.toString(), BASE_RELATION_URI, true });

				// store new relationship info
				newTriplesToAdd.add(new Object[] { newStart, newRelationURI, newEnd, true });
				newTriplesToAdd.add(new Object[] { newRelationURI, RDFS.SUBPROPERTYOF.toString(), BASE_RELATION_URI, true });
			}

		}

		if (bindings.length() > 0) {
			// get everything downstream of the props
			{
				String query = "select ?s ?p ?o where { {?s ?p ?o} } bindings ?s {" + bindings.toString() + "}";

				IRawSelectWrapper it = null;
				try {
					it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, query);
					while (it.hasNext()) {
						IHeadersDataRow headerRows = it.next();
						storeTripleToDelete(headerRows, oldTriplesToDelete);

						// add the new concept downstream props
						Object[] raw = headerRows.getRawValues();
						String s = raw[0].toString();
						String p = raw[1].toString();
						String o = raw[2].toString();
						String newS = newProperties.get(s);
						if (p.equals(PIXEL_RELATION_URI)) {
							String newO = o.substring(0, o.lastIndexOf("/") + 1);
							o = newO + newConceptName;
						}
						boolean isLiteral = objectIsLiteral(p);
						if (isLiteral) {
							newTriplesToAdd.add(new Object[] { newS, p, headerRows.getValues()[2], false });
						} else {
							newTriplesToAdd.add(new Object[] { newS, p, o, true });
						}
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if(it != null) {
						try {
							it.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}

			// repeat for upstream of prop
			{
				String query = "select ?s ?p ?o where { {?s ?p ?o} } bindings ?o {" + bindings.toString() + "}";

				IRawSelectWrapper it = null;
				try {
					it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, query);
					while (it.hasNext()) {
						IHeadersDataRow headerRows = it.next();
						storeTripleToDelete(headerRows, oldTriplesToDelete);

						// add the new concept upstream props
						Object[] raw = headerRows.getRawValues();
						String s = raw[0].toString();
						String p = raw[1].toString();
						String o = raw[2].toString();
						String newO = newProperties.get(o);
						if (s.equals(oldConceptPhysical)) {
							newTriplesToAdd.add(new Object[] { newConceptPhysicalUri, p, newO, true });
						} else {
							newTriplesToAdd.add(new Object[] { s, p, newO, true });
						}
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if(it != null) {
						try {
							it.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}
		}

		boolean hasTriple = false;

		// now repeat for the node itself
		// remove everything downstream of the node
		{
			String query = "select ?s ?p ?o where { bind(<" + oldConceptPhysical + "> as ?s) {?s ?p ?o} }";

			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, query);
				while (it.hasNext()) {
					hasTriple = true;
					IHeadersDataRow headerRows = it.next();

					// add downstream for node props
					Object[] raw = headerRows.getRawValues();

					String p = raw[1].toString();
					if (newRelations.containsKey(p)) {
						// this relation has already been modified
						continue;
					}

					String o = raw[2].toString();
					if (newProperties.containsKey(o)) {
						o = newProperties.get(o);
					} else if (o.equals(oldConceptPhysical)) {
						o = newConceptPhysicalUri;
					}
					boolean isLiteral = objectIsLiteral(p);
					if (isLiteral) {
						newTriplesToAdd
								.add(new Object[] { newConceptPhysicalUri, p, headerRows.getValues()[2], false });
					} else {
						newTriplesToAdd.add(new Object[] { newConceptPhysicalUri, p, o, true });
					}
					storeTripleToDelete(headerRows, oldTriplesToDelete);
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		// repeat for upstream of the node
		{
			String query = "select ?s ?p ?o where { bind(<" + oldConceptPhysical + "> as ?o) {?s ?p ?o} }";

			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, query);
				while (it.hasNext()) {
					hasTriple = true;
					IHeadersDataRow headerRows = it.next();

					// add for the upstream of the node
					Object[] raw = headerRows.getRawValues();

					String s = raw[0].toString();
					if (s.equals(oldConceptPhysical)) {
						s = newConceptPhysicalUri;
					} else if (newProperties.containsKey(s)) {
						s = newProperties.get(s);
					}
					String p = raw[1].toString();
					if (newRelations.containsKey(p)) {
						// this relation has already been modified
						continue;
					}
					newTriplesToAdd.add(new Object[] { s, p, newConceptPhysicalUri, true });
					storeTripleToDelete(headerRows, oldTriplesToDelete);
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		if (!hasTriple) {
			throw new IllegalArgumentException("Cannot find concept in existing metadata to remove");
		}
			
		// delete the old triples
		for (Object[] data : oldTriplesToDelete) {
			this.baseDataEngine.removeStatement(data);
		}

		for (Object[] data : newTriplesToAdd) {
			this.baseDataEngine.addStatement(data);
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully removed concept and all its dependencies", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
	/**
	 * Remove a concept from the OWL If RDF : a concept has a data type (String) If
	 * RDBMS : this will represent a table and not have a datatype
	 * 
	 * @param appId
	 * @param tableName
	 * @param oldPropName
	 * @param newPropName
	 * @return
	 */
	public NounMetadata renameProp(String tableName, String oldPropName, String newPropName) {
		// need to grab everything downstream of the node and edit it
		// need to grab everything upstream of the node and edit it
		
		String propPhysicalUri = BASE_PROPERTY_URI + "/" + oldPropName + "/" + tableName;
		String newPropPhysicalUri = BASE_PROPERTY_URI + "/" + newPropName + "/" + tableName;

		List<Object[]> newTriplesToAdd = new ArrayList<>();
		List<Object[]> oldTriplesToDelete = new ArrayList<>();

		// remove relationships to node
		List<String[]> fkRelationships = getPhysicalRelationships(this.baseDataEngine);
		String baseRelationURI = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;

		for (String[] relations: fkRelationships) {
			// track if change needs to be made
			boolean editRelation = false;
			String start = relations[0];
			String end = relations[1];
			String relURI = relations[2];
			String relationName = Utility.getInstanceName(relURI); // this is either a.b.c.d or x.a.b.y.c.d
			String[] tablesAndPrimaryKeys = relationName.split("\\.");
			
			// need to change the b or d in relationName
			if (tablesAndPrimaryKeys.length == 4) {
				String startCol = tablesAndPrimaryKeys[1];
				String endCol = tablesAndPrimaryKeys[3];
				if(startCol.equals(oldPropName)) {
					editRelation = true;
					tablesAndPrimaryKeys[1] = newPropName;
				} else if (endCol.equals(oldPropName)) {
					editRelation = true;
					tablesAndPrimaryKeys[3] = newPropName;
				}
			} else if (tablesAndPrimaryKeys.length == 6) {
				// this has the schema
				// x.a.b.y.c.d
				String startCol = tablesAndPrimaryKeys[2];
				String endCol = tablesAndPrimaryKeys[5];
				if(startCol.equals(oldPropName)) {
					editRelation = true;
					tablesAndPrimaryKeys[2] = newPropName;
				} else if (endCol.equals(oldPropName)) {
					editRelation = true;
					tablesAndPrimaryKeys[5] = newPropName;
				}
			}

			if (editRelation) {
				// create relationship name a.b.c.d or x.a.b.y.c.d
				String newRelName = String.join(".", tablesAndPrimaryKeys);
				String newRelationURI = baseRelationURI + "/" + newRelName;

				// store old relationship info
				oldTriplesToDelete.add(new Object[] { relations[0], relations[2], relations[1], true });
				oldTriplesToDelete.add(new Object[] { relations[2], RDFS.SUBPROPERTYOF.toString(), BASE_RELATION_URI, true });

				// store new relationship info
				newTriplesToAdd.add(new Object[] { start, newRelationURI, end, true });
				newTriplesToAdd.add(new Object[] { newRelationURI, RDFS.SUBPROPERTYOF.toString(), BASE_RELATION_URI, true });
			}

		}
		
		
		{
			// remove everything downstream of the property
			String downstreamQuery = "select ?s ?p ?o where { bind(<" + propPhysicalUri + "> as ?s) " + "{?s ?p ?o} }";
			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, downstreamQuery);
				while (it.hasNext()) {
					IHeadersDataRow headerRows = it.next();
					storeTripleToDelete(headerRows, oldTriplesToDelete);

					Object[] raw = headerRows.getRawValues();
					String p = raw[1].toString();
					String o = raw[2].toString();
					boolean isLiteral = objectIsLiteral(p);
					if (isLiteral) {
						newTriplesToAdd.add(new Object[] { newPropPhysicalUri, p, headerRows.getValues()[2], false });
					} else {
						newTriplesToAdd.add(new Object[] { newPropPhysicalUri, p, o, true });
					}
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		{
			// repeat for upstream of the property
			String upstreamQuery = "select ?s ?p ?o where { bind(<" + propPhysicalUri + "> as ?o) {?s ?p ?o} }";
			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, upstreamQuery);
				while (it.hasNext()) {
					IHeadersDataRow headerRows = it.next();
					storeTripleToDelete(headerRows, oldTriplesToDelete);
					
					Object[] raw = headerRows.getRawValues();
					String s = raw[0].toString();
					String p = raw[1].toString();
					newTriplesToAdd.add(new Object[] { s, p, newPropPhysicalUri, true });
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		// delete the old triples
		for (Object[] data : oldTriplesToDelete) {
			this.baseDataEngine.removeStatement(data);
		}
		
		// add new triples
		for(Object[] data : newTriplesToAdd) {
			this.baseDataEngine.addStatement(data);
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully removed property", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	

	////////////////////////////////// END REMOVING PROPERTIES TO CONCEPTS INTO THE OWL //////////////////////////////////

	////////////////////////////////// UTILITY METHODS TO REMOVE FROM OWL //////////////////////////////////

	private List<String[]> getPhysicalRelationships(IDatabaseEngine engine) {
		String query = "SELECT DISTINCT ?start ?end ?rel WHERE { "
				+ "{?start <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?end <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} " + "{?start ?rel ?end}"
				+ "Filter(?rel != <" + RDFS.SUBPROPERTYOF + ">)"
				+ "Filter(?rel != <http://semoss.org/ontologies/Relation>)" + "}";
		return Utility.getVectorArrayOfReturn(query, engine, true);
	}

	private void executeRemoveQuery(IHeadersDataRow headerRows, RDFFileSesameEngine owlEngine) {
		Object[] raw = headerRows.getRawValues();
		String s = raw[0].toString();
		String p = raw[1].toString();
		String o = raw[2].toString();
		boolean isLiteral = objectIsLiteral(p);
		if (isLiteral) {
			owlEngine.removeStatement(new Object[] { s, p, headerRows.getValues()[2], false });
		} else {
			owlEngine.removeStatement(new Object[] { s, p, o, true });
		}
	}
	
	private void storeTripleToDelete(IHeadersDataRow headerRows, List<Object[]> dataToDelete ) {
		Object[] raw = headerRows.getRawValues();
		String s = raw[0].toString();
		String p = raw[1].toString();
		String o = raw[2].toString();
		boolean isLiteral = objectIsLiteral(p);
		if (isLiteral) {
			dataToDelete.add(new Object[] { s, p, headerRows.getValues()[2], false });
		} else {
			dataToDelete.add(new Object[] { s, p, o, true });
		}
	}

	/**
	 * Determine if the predicate points to a literal
	 * 
	 * @param predicate
	 * @return
	 */
	protected boolean objectIsLiteral(String predicate) {
		Set<String> literalPreds = new HashSet<String>();

		literalPreds.add(RDFS.LABEL.toString());
		literalPreds.add(OWL.sameAs.toString());
		literalPreds.add(RDFS.COMMENT.toString());
		literalPreds.add(SEMOSS_URI_PREFIX + DEFAULT_PROP_CLASS + "/UNIQUE");
		literalPreds.add(CONCEPTUAL_RELATION_URI);
		literalPreds.add(RDFS.DOMAIN.toString());

		if (literalPreds.contains(predicate)) {
			return true;
		}
		return false;
	}
	
	/**
	 * Adding information into the base engine
	 * Currently assumes we are only adding URIs (object is never a literal)
	 * @param triple 			The triple to load into the engine and into baseDataHash
	 */
	public void addToBaseEngine(Object[] triple) {
		String sub = (String) triple[0];
		String pred = (String) triple[1];
		// is this a URI or a literal?
		boolean concept = Boolean.valueOf((boolean) triple[3]);

		String cleanSub = Utility.cleanString(sub, false);
		String cleanPred = Utility.cleanString(pred, false);
		
		Object objValue = triple[2];
		// if it is a URI
		// gotta clean up the value
		if(concept) {
			objValue = Utility.cleanString(objValue.toString(), false);
		}
		
		baseDataEngine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{cleanSub, cleanPred, objValue, concept});
	}
	
	/**
	 * Adding information into the base engine
	 * Currently assumes we are only adding URIs (object is never a literal)
	 * @param triple 			The triple to load into the engine and into baseDataHash
	 */
	public void removeFromBaseEngine(Object[] triple) {
		String sub = (String) triple[0];
		String pred = (String) triple[1];
		String obj = (String) triple[2];
		boolean concept = Boolean.valueOf((boolean) triple[3]);

		String cleanSub = Utility.cleanString(sub, false);
		String cleanPred = Utility.cleanString(pred, false);

		Object objValue = triple[2];
		// if it is a URI
		// gotta clean up the value
		if(concept) {
			objValue = Utility.cleanString(objValue.toString(), false);
		}
		
		baseDataEngine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{cleanSub, cleanPred, objValue, concept});
	}
	
	// set this as separate pieces as well
	public void addToBaseEngine(String subject, String predicate, String object) {
		addToBaseEngine(new Object[]{subject, predicate, object, true});
	}
	
	public void addToBaseEngine(String subject, String predicate, Object object, boolean isUri) {
		addToBaseEngine(new Object[]{subject, predicate, object, isUri});
	}
	
	// set this as separate pieces as well
	public void removeFromBaseEngine(String subject, String predicate, String object) {
		removeFromBaseEngine(new Object[]{subject, predicate, object, true});
	}

	public void removeFromBaseEngine(String subject, String predicate, Object object, boolean isUri) {
		removeFromBaseEngine(new Object[]{subject, predicate, object, isUri});
	}
	
	/**
	 * 
	 * @throws IOException
	 */
	public void export() throws IOException {
		export(true);
	}
	
	/**
	 * 
	 * @throws IOException
	 */
	public void export(boolean addTimeStamp) throws IOException {
		try {
			//adding a time-stamp to the OWL file
			if(addTimeStamp) {
				deleteExisitngTimestamp();
				Calendar cal = Calendar.getInstance();
				String cleanObj = DATE_FORMATTER.format(cal.getTime());
				this.baseDataEngine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{TIME_URL, TIME_KEY, cleanObj, false});
			}
			this.baseDataEngine.exportDB();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IOException("Error in writing OWL file");
		}
	}

	private void deleteExisitngTimestamp() {
		String getAllTimestampQuery = "SELECT DISTINCT ?time ?val WHERE { "
				+ "BIND(<http://semoss.org/ontologies/Concept/TimeStamp> AS ?time)"
				+ "{?time <" + TIME_KEY + "> ?val} "
				+ "}";
		
		List<String> currTimes = new ArrayList<>();

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, getAllTimestampQuery);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] rawRow = row.getRawValues();
				Object[] cleanRow = row.getValues();
				currTimes.add(rawRow[0] + "");
				currTimes.add(cleanRow[1] + "");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		for(int delIndex = 0; delIndex < currTimes.size(); delIndex+=2) {
			Object[] delTriples = new Object[4];
			delTriples[0] = currTimes.get(delIndex);
			delTriples[1] = TIME_KEY;
			delTriples[2] = currTimes.get(delIndex+1);
			delTriples[3] = false;
			
			this.baseDataEngine.doAction(ACTION_TYPE.REMOVE_STATEMENT, delTriples);
		}
	}

	/**
	 * Commits the triples added to the base engine
	 */
	public void commit() {
		baseDataEngine.commit();
	}
	
	/**
	 * 
	 * @return
	 */
	public RDFFileSesameEngine getBaseEng() {
		return this.baseDataEngine;
	}

	/**
	 * @throws IOException 
	 * 
	 */
	public void closeOwl() throws IOException {
		this.baseDataEngine.close();
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
					this.addToBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
				}
			}
		}
	}
	
	public void addLogicalNames(String physicalUri, Collection<String> logicalNames) {
		if(logicalNames != null) {
			for(String lName : logicalNames) {
				if(lName != null && !lName.isEmpty()) {
					this.addToBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
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
					this.removeFromBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
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
					this.removeFromBaseEngine(new Object[]{physicalUri, OWL.sameAs.toString(), lName, false});
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
			this.addToBaseEngine(new Object[]{physicalUri, RDFS.COMMENT.toString(), description, false});
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
			this.removeFromBaseEngine(new Object[]{physicalUri, RDFS.COMMENT.toString(), description, false});
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
		this.addToBaseEngine("SEMOSS:ENGINE_METADATA", "CONTAINS:BASE_URI", customBaseURI+"/"+DEFAULT_NODE_CLASS+"/");
	}
	
	/////////////////// END ADDITIONAL METHODS TO INSERT INTO THE OWL /////////////////////////////////
	
	
	///////////////// GETTERS ///////////////////////
	
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
	
	

	@Override
	public RDFFileSesameEngine getBaseDataEngine() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setBaseDataEngine(RDFFileSesameEngine baseDataEngine) {
		// TODO Auto-generated method stub
		
	}
}
