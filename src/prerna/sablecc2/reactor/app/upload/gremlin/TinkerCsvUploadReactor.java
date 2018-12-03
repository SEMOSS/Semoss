package prerna.sablecc2.reactor.app.upload.gremlin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.reactor.app.upload.AbstractUploadFileReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class TinkerCsvUploadReactor extends AbstractUploadFileReactor {
	
	protected final String TINKER_DRIVER_TYPE = "tinkerDriver";
	
	public TinkerCsvUploadReactor() {
		this.keysToGet = new String[] { 
				UploadInputUtility.APP, 
				UploadInputUtility.FILE_PATH,
				UploadInputUtility.ADD_TO_EXISTING,
				UploadInputUtility.DELIMITER, 
				UploadInputUtility.DATA_TYPE_MAP, 
				UploadInputUtility.NEW_HEADERS,
				UploadInputUtility.METAMODEL, 
				UploadInputUtility.PROP_FILE, 
				UploadInputUtility.ADD_TO_EXISTING,
				UploadInputUtility.START_ROW, 
				UploadInputUtility.END_ROW, 
				UploadInputUtility.ADDITIONAL_DATA_TYPES,
				TINKER_DRIVER_TYPE };
	}
	
	private CSVFileHelper helper;

	public void generateNewApp(User user, String newAppId, String newAppName, String filePath) throws Exception {
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, String> newHeaders = UploadInputUtility.getNewCsvHeaders(this.store);
		Map<String, String> additionalDataTypes = UploadInputUtility.getAdditionalCsvDataTypes(this.store);

		int stepCounter = 1;
		logger.info(stepCounter + ". Start validating app");
		UploadUtilities.validateApp(user, newAppName);
		logger.info(stepCounter + ". Done validating app");
		stepCounter++;

		logger.info(stepCounter + ". Start generating app folder");
		this.appFolder = UploadUtilities.generateAppFolder(newAppId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create metadata for app...");
		File owlFile = UploadUtilities.generateOwlFile(newAppId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create properties file for app...");
		this.tempSmss = UploadUtilities.generateTemporaryTinkerSmss(newAppId, newAppName, owlFile, getTinkerDriverType());
		DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		// get metamodel
		Map<String, Object> metamodelProps = UploadInputUtility.getMetamodelProps(this.store);
		Map<String, String> dataTypesMap = null;
		if (metamodelProps != null) {
			dataTypesMap = (Map<String, String>) metamodelProps.get(Constants.DATA_TYPES);
		}

		/*
		 * Load data into tinker engine
		 */
		logger.info(stepCounter + ". Create  Tinker app...");
		this.engine = new TinkerEngine();
		this.engine.setEngineId(newAppId);
		this.engine.setEngineName(newAppName);
		this.engine.openDB(this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start loading data..");
		this.helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(this.helper, dataTypesMap, additionalDataTypes);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		// TODO additional types?
		String[] additionalTypes = (String[]) headerTypesArr[2];
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), ENGINE_TYPE.TINKER);
		if (metamodelProps.get(Constants.DATA_TYPES) == null) {
			// put in types to metamodel
			Map<String, String> dataTypes = new HashMap<>();
			for (int i = 0; i < headers.length; i++) {
				String header = headers[i];
				String type = types[i].toString();
				dataTypes.put(header, type);
			}
			metamodelProps.put(Constants.DATA_TYPES, dataTypes);
		}
		processRelationships(this.engine, owler, this.helper, headers, types, metamodelProps);
		this.engine.commit();
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Commit app metadata...");
		owler.commit();
		owler.export();
		this.engine.setOWL(owler.getOwlPath());
		logger.info(stepCounter + ". Complete...");
		stepCounter++;

		Gson gson = new GsonBuilder().create();
		String json = gson.toJson(((TinkerEngine) this.engine).getTypeMap());
		String mapProp = "TYPE_MAP" + "\t" + json + "\n";
		json = gson.toJson(((TinkerEngine) this.engine).getNameMap());
		mapProp += "NAME_MAP" + "\t" + json + "\n";

		// TODO add additional tinker properties to smss
		Files.write(Paths.get(this.tempSmss.getAbsolutePath()), mapProp.getBytes(), StandardOpenOption.APPEND);

		// and rename .temp to .smss
		this.smssFile = new File(this.tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		FileUtils.copyFile(this.tempSmss, this.smssFile);

		this.tempSmss.delete();
		this.engine.setPropFile(this.smssFile.getAbsolutePath());
		UploadUtilities.updateDIHelper(newAppId, newAppName, this.engine, this.smssFile);

		logger.info(stepCounter + ". Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppId, newAppName);
		UploadUtilities.addExploreInstanceInsight(newAppId, insightDatabase);
		this.engine.setInsightDatabase(insightDatabase);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Process app metadata to allow for traversing across apps");
		UploadUtilities.updateMetadata(newAppId);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Save csv metamodel prop file	");
		UploadUtilities.createPropFile(newAppId, newAppName, filePath, metamodelProps);
		logger.info(stepCounter + ". Complete");
		stepCounter++;
	}
	
	//TODO
	//TODO
	//TODO
	//TODO
	//TODO
	//TODO
	public void addToExistingApp(String appId, String filePath) throws Exception {
		int stepCounter = 1;
		logger.info(stepCounter + ". Get existing app..");
		this.engine = Utility.getEngine(appId);
		if (!(this.engine instanceof TinkerEngine)) {
			throw new IllegalArgumentException("Invalid app type");
		}
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Get app upload input...");
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, String> newHeaders = UploadInputUtility.getNewCsvHeaders(this.store);
		Map<String, String> additionalDataTypes = UploadInputUtility.getAdditionalCsvDataTypes(this.store);
		Map<String, Object> metamodelProps = UploadInputUtility.getMetamodelProps(this.store);
		Map<String, String> dataTypesMap = null;
		if (metamodelProps != null) {
			dataTypesMap = (Map<String, String>) metamodelProps.get(Constants.DATA_TYPES);
		}
		logger.info(stepCounter + ". Done...");
		stepCounter++;

		logger.info(stepCounter + "Parsing file metadata...");
		this.helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		// get the user selected datatypes for each header
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(this.helper, dataTypesMap, additionalDataTypes);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start loading data..");
		OWLER owler = new OWLER(this.engine, this.engine.getOWL());

		if (metamodelProps.get(Constants.DATA_TYPES) == null) {
			// put in types to metamodel
			Map<String, String> dataTypes = new HashMap<>();
			for (int i = 0; i < headers.length; i++) {
				String header = headers[i];
				String type = types[i].toString();
				dataTypes.put(header, type);
			}
			metamodelProps.put(Constants.DATA_TYPES, dataTypes);
		}
		processRelationships(this.engine, owler, this.helper, headers, types, metamodelProps);
		this.engine.commit();
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.warn(stepCounter + ". Committing app metadata....");
		owler.commit();
		owler.export();
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		// TODO generate new app insights for tinker
		// TODO update type map and node name map in smss

		logger.info(stepCounter + ". Process app metadata to allow for traversing across apps	");
		UploadUtilities.updateMetadata(this.engine.getEngineId());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Save csv metamodel prop file	");
		UploadUtilities.createPropFile(appId, this.engine.getEngineName(), filePath, metamodelProps);
		logger.info(stepCounter + ". Complete");
	}

	@Override
	public void closeFileHelpers() {
		if (this.helper != null) {
			this.helper.clear();
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
	 * @param metamodel 
	 * 
	 * @throws IOException
	 */
	private void processRelationships(IEngine engine, OWLER owler, CSVFileHelper csvHelper, String[] headers, SemossDataType[] types, Map<String, Object> metamodel) {
		// get all the relation
		// overwrite this value if user specified the max rows to load
		List<String> relationList = new ArrayList<String>();
		List<String> nodePropList = new ArrayList<String>();
		List<String> relPropList = new ArrayList<String>();
		parseMetamodel(metamodel, owler, relationList, nodePropList, relPropList);

		// only start from the maxRow - the startRow
		// added -1 is because of index nature
		// the earlier rows should already have been skipped
		String[] values = null;

		int startRow = (int) metamodel.get(Constants.START_ROW);
		// skip rows
		// start count at 1 just row 1 is the header
		int count = 1;
		while (count < startRow - 1 && csvHelper.getNextRow() != null) {
			count++;
		}
		Integer endRow = (Integer) metamodel.get(Constants.END_ROW);
		if (endRow == null) {
			endRow = UploadInputUtility.END_ROW_INT;
		}
		while ((values = csvHelper.getNextRow()) != null && count < (endRow)) {
			count++;
			// logger.info("Process line: " + count);

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
				// if (rdfMap.containsKey(sub)) {
				// String userSub = rdfMap.get(sub).toString();
				// subject = userSub.substring(userSub.lastIndexOf("/") + 1);
				// }
				// // if no user specified URI, use generic URI
				// else {
				if (sub.contains("+")) {
					subject = processAutoConcat(sub);
				} else {
					subject = sub;
				}
				// }
				// see if object node URI exists in prop file
				// if (rdfMap.containsKey(obj)) {
				// String userObj = rdfMap.get(obj).toString();
				// object = userObj.substring(userObj.lastIndexOf("/") + 1);
				// }
				// // if no user specified URI, use generic URI
				// else {
				if (obj.contains("+")) {
					object = processAutoConcat(obj);
				} else {
					object = obj;
				}
				// }

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
							// // see if property node URI exists in prop file
							// if (rdfMap.containsKey(prop)) {
							// String userProp = rdfMap.get(prop).toString();
//								property = userProp.substring(userProp.lastIndexOf("/") + 1);
//
//								// property = rdfMap.get(prop);
//							}
							// // if no user specified URI, use generic URI
							// else {
							if (prop.contains("+")) {
								property = processAutoConcat(prop);
							} else {
								property = prop;
							}
							// }
							propHash.put(property, createObject(prop, values, types, headers));
						}
					}
				}
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
				// if (rdfMap.containsKey(sub)) {
				// String userSub = rdfMap.get(sub).toString();
				// subject = userSub.substring(userSub.lastIndexOf("/") + 1);
				//
				// // subject = rdfMap.get(sub);
				// }
				// // if no user specified URI, use generic URI
				// else {
				if (sub.contains("+")) {
					subject = processAutoConcat(sub);
				} else {
					subject = sub;
				}
				// }
				String subjectValue = createInstanceValue(sub, values, headers);
				// loop through all properties on the node
				for (int i = 1; i < strSplit.length; i++) {
					String prop = strSplit[i].trim();
					String property = "";
					// see if property node URI exists in prop file
					// if (rdfMap.containsKey(prop)) {
					// String userProp = rdfMap.get(prop).toString();
					// property = userProp.substring(userProp.lastIndexOf("/") +
					// 1);
					//
					// // property = rdfMap.get(prop);
					// }
					// // if no user specified URI, use generic URI
					// else {
					if (prop.contains("+")) {
						property = processAutoConcat(prop);
					} else {
						property = prop;
					}
					// }
					Object propObj = createObject(prop, values, types, headers);
					if (propObj == null || propObj.toString().isEmpty()) {
						continue;
					}
					nodePropHash.put(property, propObj);
				}
				addNodeProperties(owler, engine, subject, subjectValue, nodePropHash);
			}
		}
		metamodel.put(Constants.END_ROW, count);
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

	private void parseMetamodel(Map<String, Object> metamodel, OWLER owler, List<String> relationList, List<String> nodePropList, List<String> relPropList) {
		Set<String> concepts = new HashSet<>();
		Map dataTypeMap = (Map) metamodel.get(Constants.DATA_TYPES);
		if (metamodel.get(Constants.RELATION) != null) {
			List<Map<String, Object>> edgeList = (List<Map<String, Object>>) metamodel.get(Constants.RELATION);
			// process each relationship
			for (Map relMap : edgeList) {
				String subject = (String) relMap.get(Constants.FROM_TABLE);
				String object = (String) relMap.get(Constants.TO_TABLE);
				String predicate = (String) relMap.get(Constants.REL_NAME);
				owler.addConcept(subject);
				concepts.add(subject);
				owler.addConcept(object);
				concepts.add(object);
				owler.addRelation(subject, object, predicate);
				String relation = subject + "@" + predicate + "@" + object;
				relationList.add(relation);
			}
		}

		if (metamodel.get(Constants.NODE_PROP) != null) {
			Map<String, Object> nodeProps = (Map<String, Object>) metamodel.get(Constants.NODE_PROP);
			for (String concept : nodeProps.keySet()) {
				List<String> conceptProps = (List<String>) nodeProps.get(concept);
				for (String property : conceptProps) {
					String relation = concept + "%" + property;
					nodePropList.add(relation);
					String dataType = (String) dataTypeMap.get(property);
					owler.addProp(concept, property, dataType);
					concepts.add(concept);
				}
			}
		}

		if (metamodel.get(Constants.RELATION_PROP) != null) {
			Map<String, Object> relProps = (Map<String, Object>) metamodel.get(Constants.RELATION_PROP);
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

		startV = (Vertex) engine.doAction(IEngine.ACTION_TYPE.VERTEX_UPSERT, new Object[] { subjectNodeType, instanceSubjectName });

		// upsert the object vertex
		Vertex endV = (Vertex) engine.doAction(IEngine.ACTION_TYPE.VERTEX_UPSERT, new Object[] { objectNodeType, instanceObjectName });

		// upsert the edge between them
		engine.doAction(IEngine.ACTION_TYPE.EDGE_UPSERT, new Object[] { startV, subjectNodeType, endV, objectNodeType, propHash });

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

	////////////////////////////////////////////
	// Specific Tinker Engine inputs
	////////////////////////////////////////////
	private TINKER_DRIVER getTinkerDriverType() {
		GenRowStruct grs = this.store.getNoun(TINKER_DRIVER_TYPE);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return TINKER_DRIVER.valueOf((String) grs.get(0));
	}
}
