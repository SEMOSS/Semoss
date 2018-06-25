package prerna.sablecc2.reactor.app.upload.gremlin;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.upload.AbstractRdbmsUploadReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class UploadTinkerEngine extends AbstractRdbmsUploadReactor {
	protected final String TINKER_DRIVER_TYPE = "tinkerDriver";

	private static final String CLASS_NAME = UploadTinkerEngine.class.getName();

	public UploadTinkerEngine() {
		this.keysToGet = new String[] { UploadInputUtility.APP, UploadInputUtility.FILE_PATH, UploadInputUtility.DELIMITER, DATA_TYPE_MAP, NEW_HEADERS, "rdfProp",
				TINKER_DRIVER_TYPE, "metamodel", ADDITIONAL_TYPES };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		String appIdOrName = UploadInputUtility.getAppName(this.store);
		String filePath = UploadInputUtility.getFilePath(this.store);
		String returnId = null;
		final boolean existing = UploadInputUtility.getExisting(this.store);
		if(existing) {
			//TODO
			returnId = addToExistingApp(appIdOrName, filePath, logger);
		} else {
			returnId = generateNewApp(appIdOrName, filePath, logger);
		}
		return new NounMetadata(returnId, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	@Override
	public String generateNewApp(String newAppName, String filePath, Logger logger) {
		/*
		 * Things we need to do 
		 * 1) make directory
		 * 2) make owl 
		 * 3) make temporary smss 
		 * 4) make engine class 
		 * 5) load actual data 
		 * 6) load owl metadata 
		 * 7) load default insights 
		 * 8) add to localmaster and solr
		 */

		String newAppId = UUID.randomUUID().toString();
		logger.info("Start validating app");
		try {
			UploadUtilities.validateApp(newAppName);
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("Done validating app");

		logger.info("Starting app creation");
		logger.info("1. Start generating app folder");
		UploadUtilities.generateAppFolder(newAppId, newAppName);
		logger.info("1. Complete");

		logger.info("Generate new app database");
		logger.info("2. Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(newAppId, newAppName);
		logger.info("2. Complete");

		logger.info("3. Create properties file for database...");
		File tempSmss = null;
		try {
			tempSmss = UploadUtilities.generateTemporaryTinkerSmss(newAppId, newAppName, owlFile, getTinkerDriverType());
			DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("3. Complete");
		
		logger.info("4. Start loading data..");
		logger.info("Parsing file metadata...");
		TinkerEngine engine = new TinkerEngine();
		engine.setEngineId(newAppId);
		engine.setEngineName(newAppName);
		engine.openDB(tempSmss.getAbsolutePath());
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), ENGINE_TYPE.TINKER);

		try {
			insertData( engine, owler, filePath, logger);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		logger.info("4. Complete");

		logger.info("5. Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppId, newAppName);
		UploadUtilities.addExploreInstanceInsight(newAppId, insightDatabase);
		engine.setInsightDatabase(insightDatabase);
		logger.info("5. Complete");

		logger.info("6. Process app metadata to allow for traversing across apps	");
		try {
			UploadUtilities.updateMetadata(newAppId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("6. Complete");

		Gson gson = new GsonBuilder().create();
		String json = gson.toJson(engine.getTypeMap());
		String mapProp = "TYPE_MAP" + "\t" + json + "\n";
		json = gson.toJson(engine.getNameMap());
		mapProp += "NAME_MAP" + "\t" + json + "\n";

		try {
			Files.write(Paths.get(tempSmss.getAbsolutePath()), mapProp.getBytes(), StandardOpenOption.APPEND);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// and rename .temp to .smss
		File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempSmss, smssFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		tempSmss.delete();

		// update DIHelper & engine smss file location
		engine.setPropFile(smssFile.getAbsolutePath());
		UploadUtilities.updateDIHelper(newAppId, newAppName, engine, smssFile);
		return newAppId;
	}

	/**
	 * 
	 * @param owler 
	 * @param fileNames
	 * @param owlFileLocation
	 * @param logger
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void insertData(IEngine engine, OWLER owler, String fileNames, Logger logger)
			throws FileNotFoundException, IOException {

		// String[] files = prepareCsvReader(fileNames, customBase, owlFileLocation, smssLocation, propertyFiles);
		String[] files = fileNames.trim().split(";");

		try {
//			openTinkerEngineWithoutConnection(appName, appID);
			for (int i = 0; i < files.length; i++) {
				CSVFileHelper helper = null;
				try {
					String fileName = files[i];
					// open the csv file
					// and get the headers
					helper = UploadUtilities.getHelper(fileName, UploadInputUtility.getDelimiter(this.store), getDataTypeMap(), getNewHeaders());
					// load the prop file for the CSV file
//					if (propFileExist) {
//						// if we have multiple files, load the correct one
//						if (propFiles != null) {
//							propFile = propFiles[i];
//						}
//						openProp(propFile);
//					} else {
//						rdfMap = rdfMapArr[i];
//					}
					
					
					// get the user selected datatypes for each header
//					preParseRdfCSVMetaData(rdfMap);
					Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(helper, getDataTypeMap(), getAdditionalTypes());
					String[] headers = (String[]) headerTypesArr[0];
					SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
					//TODO
					String[] additionalTypes = (String[]) headerTypesArr[2];
					List<String> relationList = new ArrayList<String>();
					List<String> nodePropList = new ArrayList<String>();
					List<String> relPropList = new ArrayList<String>();
					parseMetadata(getMetamodel(), owler, relationList, nodePropList, relPropList);
					processRelationShips(engine, owler, helper, logger, relationList, nodePropList, relPropList, headers, types);
				} finally {
//					closeCSVFile();
					if(helper != null) {
					helper.clear();
					}

				}
			}
			// loadMetadataIntoEngine();
			owler.commit();
			try {
				owler.export();
			} catch (IOException ex) {
				ex.printStackTrace();
				throw new IOException("Unable to export OWL file...");
			}
		} catch (FileNotFoundException e) {
			throw new FileNotFoundException(e.getMessage());
		} catch (IOException e) {
			throw new IOException(e.getMessage());
		} finally {
//			if (error || autoLoad) {
//				closeDB();
//				closeOWL();
//			} else {
//				commitDB();
//				
//			}
			engine.commit();
		
		}

	}
	
	/**
	 * Create all the triples associated with the relationships specified in the
	 * prop file
	 * @param relPropList 
	 * @param nodePropList 
	 * @param relationList 
	 * @param types 
	 * @param headers 
	 * 
	 * @throws IOException
	 */
	private void processRelationShips(IEngine engine, OWLER owler, CSVFileHelper csvHelper, Logger logger, List<String> relationList, List<String> nodePropList, List<String> relPropList, String[] headers, SemossDataType[] types) throws IOException {
		// get all the relation
		// overwrite this value if user specified the max rows to load


		// only start from the maxRow - the startRow
		// added -1 is because of index nature
		// the earlier rows should already have been skipped
		String[] values = null;
		int count = 0;
		while ((values = csvHelper.getNextRow()) != null ) {
			count++;
//			logger.info("Process line: " + count);

			// process all relationships in row
			for (int relIndex = 0; relIndex < relationList.size(); relIndex++) {
				String relation = relationList.get(relIndex);
				String[] strSplit = relation.split("@");
				// get the subject and object for triple (the two indexes)
				String sub = strSplit[0].trim();
				String subject = "";
				String predicate = strSplit[1].trim();
				String obj = strSplit[2].trim();
				String object = "";

				// see if subject node URI exists in prop file
//				if (rdfMap.containsKey(sub)) {
//					String userSub = rdfMap.get(sub).toString();
//					subject = userSub.substring(userSub.lastIndexOf("/") + 1);
//				}
//				// if no user specified URI, use generic URI
//				else {
					if (sub.contains("+")) {
						subject = processAutoConcat(sub);
					} else {
						subject = sub;
					}
//				}
				// see if object node URI exists in prop file
//				if (rdfMap.containsKey(obj)) {
//					String userObj = rdfMap.get(obj).toString();
//					object = userObj.substring(userObj.lastIndexOf("/") + 1);
//				}
//				// if no user specified URI, use generic URI
//				else {
					if (obj.contains("+")) {
						object = processAutoConcat(obj);
					} else {
						object = obj;
					}
//				}

				String subjectValue = createInstanceValue(sub, values, headers);
				String objectValue = createInstanceValue(obj, values, headers);
				if (subjectValue.isEmpty() || objectValue.isEmpty()) {
					continue;
				}

				// look through all relationship properties for the specific
				// relationship
				Hashtable<String, Object> propHash = new Hashtable<String, Object>();

				for (int relPropIndex = 0; relPropIndex < relPropList.size(); relPropIndex++) {
					String relProp = relPropList.get(relPropIndex);
					String[] relPropSplit = relProp.split("%");
					if (relPropSplit[0].equals(relation)) {
						// loop through all properties on the relationship
						for (int i = 1; i < relPropSplit.length; i++) {
							// add the necessary triples for the relationship
							// property
							String prop = relPropSplit[i];
							String property = "";
//							// see if property node URI exists in prop file
//							if (rdfMap.containsKey(prop)) {
//								String userProp = rdfMap.get(prop).toString();
//								property = userProp.substring(userProp.lastIndexOf("/") + 1);
//
//								// property = rdfMap.get(prop);
//							}
//							// if no user specified URI, use generic URI
//							else {
								if (prop.contains("+")) {
									property = processAutoConcat(prop);
								} else {
									property = prop;
								}
//							}
							propHash.put(property, createObject(prop, values, types, headers));
						}
					}
				}
				// TODO: override this
				// default is RDF
				createRelationship(engine, subject, object, subjectValue, objectValue, predicate, propHash);
			}

			// look through all node properties
			for (int relIndex = 0; relIndex < nodePropList.size(); relIndex++) {
				Hashtable<String, Object> nodePropHash = new Hashtable<String, Object>();
				String relation = nodePropList.get(relIndex);
				String[] strSplit = relation.split("%");
				// get the subject (the first index) and objects for triple
				String sub = strSplit[0].trim();
				String subject = "";
				// see if subject node URI exists in prop file
//				if (rdfMap.containsKey(sub)) {
//					String userSub = rdfMap.get(sub).toString();
//					subject = userSub.substring(userSub.lastIndexOf("/") + 1);
//
//					// subject = rdfMap.get(sub);
//				}
//				// if no user specified URI, use generic URI
//				else {
					if (sub.contains("+")) {
						subject = processAutoConcat(sub);
					} else {
						subject = sub;
					}
//				}
				String subjectValue = createInstanceValue(sub, values, headers);
				// loop through all properties on the node
				for (int i = 1; i < strSplit.length; i++) {
					String prop = strSplit[i].trim();
					String property = "";
					// see if property node URI exists in prop file
//					if (rdfMap.containsKey(prop)) {
//						String userProp = rdfMap.get(prop).toString();
//						property = userProp.substring(userProp.lastIndexOf("/") + 1);
//
//						// property = rdfMap.get(prop);
//					}
//					// if no user specified URI, use generic URI
//					else {
						if (prop.contains("+")) {
							property = processAutoConcat(prop);
						} else {
							property = prop;
						}
//					}
					Object propObj = createObject(prop, values, types, headers);
					if (propObj == null || propObj.toString().isEmpty()) {
						continue;
					}
					nodePropHash.put(property, propObj);
				}
				// TODO: override this
				// default is RDF
				addNodeProperties(owler, engine, subject, subjectValue, nodePropHash);
			}
		}
		logger.info("FINAL COUNT " + count);
	}

	public String getInstanceURI(String nodeType) {
		String customBaseURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		return customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS + "/" + nodeType;
	}

	public void addNodeProperties(OWLER owler, IEngine engine, String nodeType, String instanceName,
			Hashtable<String, Object> propHash) {
		// create the node in case its not in a relationship
		instanceName = Utility.cleanString(instanceName, true);
		nodeType = Utility.cleanString(nodeType, true);
		String semossBaseURI = owler.addConcept(nodeType);
		String instanceBaseURI = getInstanceURI(nodeType);
		String subjectNodeURI = instanceBaseURI + "/" + instanceName;
		Vertex vert = (Vertex) engine.doAction(IEngine.ACTION_TYPE.VERTEX_UPSERT,
				new Object[] { nodeType, instanceName });
		Set<String> vertProps = vert.keys();
		for (String key : propHash.keySet()) {
			if (!vertProps.contains(key)) {
				vert.property(key, propHash.get(key));
			}
		}

	}
	
	/**
	 * Gets the instance value for a given subject. The subject can be a
	 * concatenation. Note that we do not care about the data type for this
	 * since a URI is always a string
	 * 
	 * @param subject
	 *            The subject type (i.e. concept, or header name) to get the
	 *            instance value for
	 * @param values
	 *            String[] containing the values for the row
	 * @param headers
	 *            Map containing the header names to index within the values
	 *            array
	 * @return The return is the value for the instance
	 */
	private String createInstanceValue(String subject, String[] values, String[] headers) {
		String retString = "";
		// if node is a concatenation
		if (subject.contains("+")) {
			String elements[] = subject.split("\\+");
			for (int i = 0; i < elements.length; i++) {
				String subjectElement = elements[i];

				int colIndex = Arrays.asList(headers).indexOf(subjectElement);
				if (values[colIndex] != null && !values[colIndex].toString().trim().isEmpty()) {
					String value = values[colIndex] + "";
					value = Utility.cleanString(value, true);

					retString = retString + value + "-";
				} else {
					retString = retString + "null-";
				}
			}
			// a - will show up at the end of this and we need to get rid of
			// that
			if (!retString.equals(""))
				retString = retString.substring(0, retString.length() - 1);
		} else {
			// if the value is not empty, get the correct value to return
			int colIndex = Arrays.asList(headers).indexOf(subject);
			if (values[colIndex] != null && !values[colIndex].trim().isEmpty()) {
				retString = Utility.cleanString(values[colIndex], true);
			}
		}
		return retString;
	}

	private void parseMetadata(Map<String, Object> metamodel, OWLER owler, List<String> relationList,
			List<String> nodePropList, List<String> relPropList) {
		Set<String> concepts = new HashSet<>();
		if (metamodel.get("edges") != null) {
			List<Map<String, Object>> edgeList = (List<Map<String, Object>>) metamodel.get("edges");
			// process each relationship
			for (Map relMap : edgeList) {
				String subject = (String) relMap.get("fromTable");
				String object = (String) relMap.get("toTable");
				String predicate = (String) relMap.get("relName");
				owler.addConcept(subject);
				concepts.add(subject);
				owler.addConcept(object);
				concepts.add(object);
				owler.addRelation(subject, object, predicate);
				String relation = subject + "@" + predicate + "@" + object;
				relationList.add(relation);
			}
		}

		if (metamodel.get("nodes") != null) {
			Map<String, Object> nodeProps = (Map<String, Object>) metamodel.get("nodes");
			for (String concept : nodeProps.keySet()) {
				List<String> conceptProps = (List<String>) nodeProps.get(concept);
				for (String property : conceptProps) {
					String relation = concept + "%" + property;
					nodePropList.add(relation);
					owler.addProp(concept, property, getDataTypeMap().get(property));
					concepts.add(concept);
				}
			}
		}

		if (metamodel.get("RELATION_PROP") != null) {
			Map<String, Object> relProps = (Map<String, Object>) metamodel.get("RELATION_PROP");
			for (String relation : relProps.keySet()) {
				relPropList.add(relation);
			}
		}
	}

	public void createRelationship(IEngine engine, String subjectNodeType, // Title
			String objectNodeType, // Producer
			String instanceSubjectName, // Avatar
			String instanceObjectName, // James Cameron
			String relName, // Produced_By
			Hashtable<String, Object> propHash) {
		subjectNodeType = Utility.cleanString(subjectNodeType, true);
		objectNodeType = Utility.cleanString(objectNodeType, true);

		instanceSubjectName = Utility.cleanString(instanceSubjectName, true);
		instanceObjectName = Utility.cleanString(instanceObjectName, true);

		// upsert the subject vertex
		Vertex startV = null;
		try {
			startV = (Vertex) engine.doAction(IEngine.ACTION_TYPE.VERTEX_UPSERT,
					new Object[] { subjectNodeType, instanceSubjectName });
		} catch (Exception e) {
			e.printStackTrace();
		}
		// upsert the object vertex
		Vertex endV = (Vertex) engine.doAction(IEngine.ACTION_TYPE.VERTEX_UPSERT,
				new Object[] { objectNodeType, instanceObjectName });

		// upsert the edge between them
		engine.doAction(IEngine.ACTION_TYPE.EDGE_UPSERT,
				new Object[] { startV, subjectNodeType, endV, objectNodeType, propHash });

	}

	/**
	 * Gets the properly formatted object from the string[] values object Also
	 * handles if the column is a concatenation
	 * 
	 * @param object
	 *            The column to get the correct data type for - can be a
	 *            concatenation
	 * @param values
	 *            The string[] containing the values for the row
	 * @param types
	 *            The string[] containing the data type for each column in the
	 *            values array
	 * @param colNameToIndex
	 *            Map containing the column name to index in values[] for fast
	 *            retrieval of data
	 * @return The object in the correct data format
	 */
	private Object createObject(String object, String[] values, SemossDataType[] types, String[] headers) {
		// if it contains a plus sign, it is a concatenation
		if (object.contains("+")) {
			StringBuilder strBuilder = new StringBuilder();
			String[] objList = object.split("\\+");
			for (int i = 0; i < objList.length; i++) {

				strBuilder.append(values[Arrays.asList(headers).indexOf(objList[i])]);
			}
			return Utility.cleanString(strBuilder.toString(), true);
		}

		// here we need to grab the value and cast it based on the type
		Object retObj = null;
		int colIndex = Arrays.asList(headers).indexOf(object);

		SemossDataType type = types[colIndex];
		String strVal = values[colIndex];
		if (type == SemossDataType.INT || type == SemossDataType.DOUBLE) {
			retObj = Utility.getDouble(strVal);
		} else if (type == SemossDataType.DATE) {
			retObj = Utility.getDateAsDateObj(strVal);
		} else {
			retObj = strVal;
		}

		return retObj;
	}

	/**
	 * Change the name of nodes that are concatenations of multiple CSV columns
	 * Example: changes the string "Cat+Dog" into "CatDog"
	 * 
	 * @param input
	 *            String name of the node that is a concatenation
	 * @return String name of the node removing the "+" to indicate a
	 *         concatenation
	 */
	private String processAutoConcat(String input) {
		String[] split = input.split("\\+");
		String output = "";
		for (int i = 0; i < split.length; i++) {
			output = output + split[i].trim();
		}
		return Utility.cleanString(output, true);
	}

	private Map<String, String> getDataTypeMap() {
		GenRowStruct grs = this.store.getNoun(DATA_TYPE_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	private Map<String, String> getNewHeaders() {
		GenRowStruct grs = this.store.getNoun(NEW_HEADERS);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}
	private Map<String, String> getAdditionalTypes() {
		GenRowStruct grs = this.store.getNoun(ADDITIONAL_TYPES);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	private TINKER_DRIVER getTinkerDriverType() {
		GenRowStruct grs = this.store.getNoun(TINKER_DRIVER_TYPE);
		if (grs == null || grs.isEmpty()) {
			return null;
		}

		return TINKER_DRIVER.valueOf((String) grs.get(0));
	}

	private Map<String, Object> getMetamodel() {
		GenRowStruct grs = this.store.getNoun("metamodel");
		if (grs == null || grs.isEmpty()) {
			return null;
		}

		return (Map<String, Object>) grs.get(0);
	}



	//TODO
	//TODO
	//TODO
	//TODO
	//TODO
	//TODO
	@Override
	public String addToExistingApp(String appId, String filePath, Logger logger) {
		appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
		IEngine engine = Utility.getEngine(appId);
		if(engine == null) {
			throw new IllegalArgumentException("Couldn't find the app " + appId + " to append data into");
		}
		if(!(engine instanceof TinkerEngine)) {
			throw new IllegalArgumentException("App must be using a Tinker database");
		}
		// get existing owl
		OWLER owler = new OWLER(engine, engine.getOWL());
		// need to get existing relationships
		try {
			insertData( engine, owler, filePath, logger);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			UploadUtilities.updateMetadata(appId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return appId;
	}

}
