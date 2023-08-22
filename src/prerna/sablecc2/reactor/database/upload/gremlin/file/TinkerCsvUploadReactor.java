package prerna.sablecc2.reactor.database.upload.gremlin.file;

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

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.reactor.database.upload.AbstractUploadFileReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class TinkerCsvUploadReactor extends AbstractUploadFileReactor {
	
	protected final String TINKER_DRIVER_TYPE = "tinkerDriver";
	
	public TinkerCsvUploadReactor() {
		this.keysToGet = new String[] { 
				UploadInputUtility.DATABASE, 
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

	public void generateNewDatabase(User user, String newDatabaseName, String filePath) throws Exception {
		final String delimiter = UploadInputUtility.getDelimiter(this.store);

		int stepCounter = 1;
		logger.info(stepCounter + ". Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(this.databaseId, newDatabaseName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create properties file for database...");
		this.tempSmss = UploadUtilities.generateTemporaryTinkerSmss(this.databaseId, newDatabaseName, owlFile, getTinkerDriverType());
		DIHelper.getInstance().setEngineProperty(this.databaseId + "_" + Constants.STORE, this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		// get metamodel
		Map<String, Object> metamodelProps = UploadInputUtility.getMetamodelProps(this.store, this.insight);
		Map<String, String> dataTypesMap = (Map<String, String>) metamodelProps.get(Constants.DATA_TYPES);

		/*
		 * Load data into tinker database
		 */
		logger.info(stepCounter + ". Create  Tinker database...");
		this.database = new TinkerEngine();
		this.database.setEngineId(this.databaseId);
		this.database.setEngineName(newDatabaseName);
		this.database.openDB(this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start loading data..");
		this.helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, (Map<String, String>) metamodelProps.get(UploadInputUtility.NEW_HEADERS));
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(this.helper, dataTypesMap, (Map<String, String>) metamodelProps.get(UploadInputUtility.ADDITIONAL_DATA_TYPES));
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		// TODO additional types?
		String[] additionalTypes = (String[]) headerTypesArr[2];
		Owler owler = new Owler(owlFile.getAbsolutePath(), DATABASE_TYPE.TINKER);
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
		processRelationships(this.database, owler, this.helper, Arrays.asList(headers), types, metamodelProps);
		this.database.commit();
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Commit database metadata...");
		// add the owl metadata
		UploadUtilities.insertOwlMetadataToGraphicalEngine(owler, (Map<String, List<String>>) metamodelProps.get(Constants.NODE_PROP), 
				UploadInputUtility.getCsvDescriptions(this.store), UploadInputUtility.getCsvLogicalNames(this.store));
		owler.commit();
		owler.export();
		this.database.setOWL(owler.getOwlPath());
		logger.info(stepCounter + ". Complete...");
		stepCounter++;

		Gson gson = new GsonBuilder().create();
		String json = gson.toJson(((TinkerEngine) this.database).getTypeMap());
		String mapProp = "TYPE_MAP" + "\t" + json + "\n";
		json = gson.toJson(((TinkerEngine) this.database).getNameMap());
		mapProp += "NAME_MAP" + "\t" + json + "\n";
		Files.write(Paths.get(this.tempSmss.getAbsolutePath()), mapProp.getBytes(), StandardOpenOption.APPEND);

//		logger.info(stepCounter + ". Start generating default database insights");
//		// note, on database creation, we auto create an insights database + add explore an instance
//		// TODO: should add some new ones...
//		logger.info(stepCounter + ". Complete");
//		stepCounter++;

		logger.info(stepCounter + ". Save csv metamodel prop file	");
		UploadUtilities.createPropFile(this.databaseId, newDatabaseName, filePath, metamodelProps);
		logger.info(stepCounter + ". Complete");
	}
	
	public void addToExistingDatabase(String filePath) throws Exception {
		if (!(this.database instanceof TinkerEngine)) {
			throw new IllegalArgumentException("Invalid database type");
		}
		
		int stepCounter = 1;
		logger.info(stepCounter + ". Get database upload input...");
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, Object> metamodelProps = UploadInputUtility.getMetamodelProps(this.store, this.insight);
		Map<String, String> dataTypesMap = (Map<String, String>) metamodelProps.get(Constants.DATA_TYPES);;
		logger.info(stepCounter + ". Done...");
		stepCounter++;

		logger.info(stepCounter + "Parsing file metadata...");
		this.helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, (Map<String, String>) metamodelProps.get(UploadInputUtility.NEW_HEADERS));
		// get the user selected datatypes for each header
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(this.helper, dataTypesMap, (Map<String, String>) metamodelProps.get(UploadInputUtility.ADDITIONAL_DATA_TYPES));
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start loading data..");
		Owler owler = new Owler(this.database);

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
		processRelationships(this.database, owler, this.helper, Arrays.asList(headers), types, metamodelProps);
		this.database.commit();
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.warn(stepCounter + ". Committing database metadata....");
		// add the owl metadata
		UploadUtilities.insertOwlMetadataToGraphicalEngine(owler, (Map<String, List<String>>) metamodelProps.get(Constants.NODE_PROP), 
				UploadInputUtility.getCsvDescriptions(this.store), UploadInputUtility.getCsvLogicalNames(this.store));
		owler.commit();
		owler.export();
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		// TODO generate new database insights for tinker
		// TODO update type map and node name map in smss

		logger.info(stepCounter + ". Save csv metamodel prop file	");
		UploadUtilities.createPropFile(this.databaseId, this.database.getEngineName(), filePath, metamodelProps);
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
	private void processRelationships(IDatabaseEngine database, Owler owler, CSVFileHelper csvHelper, List<String> headers, SemossDataType[] types, Map<String, Object> metamodel) {
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
							propHash.put(property, CSVFileHelper.createObject(prop, values, types, headers));
						}
					}
				}
				createRelationship(database, subject, object, subjectValue, objectValue, predicate, propHash);
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
					Object propObj = CSVFileHelper.createObject(prop, values, types, headers);
					if (propObj == null || propObj.toString().isEmpty()) {
						continue;
					}
					nodePropHash.put(property, propObj);
				}
				addNodeProperties(owler, database, subject, subjectValue, nodePropHash);
			}
		}
		metamodel.put(Constants.END_ROW, count);
		logger.info("FINAL COUNT " + count);
	}

	public String getInstanceURI(String nodeType) {
		String customBaseURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		return customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS + "/" + nodeType;
	}

	public void addNodeProperties(Owler owler, IDatabaseEngine database, String nodeType, String instanceName,
			Hashtable<String, Object> propHash) {
		// create the node in case its not in a relationship
		instanceName = Utility.cleanString(instanceName, true);
		nodeType = Utility.cleanString(nodeType, true);
		String semossBaseURI = owler.addConcept(nodeType);
		String instanceBaseURI = getInstanceURI(nodeType);
		String subjectNodeURI = instanceBaseURI + "/" + instanceName;
		Vertex vert = (Vertex) database.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT, new Object[] { nodeType, instanceName });
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
	private String createInstanceValue(String subject, String[] values, List<String> headers) {
		String retString = "";
		// if node is a concatenation
		if (subject.contains("+")) {
			String elements[] = subject.split("\\+");
			for (int i = 0; i < elements.length; i++) {
				String subjectElement = elements[i];

				int colIndex = headers.indexOf(subjectElement);
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
			int colIndex = headers.indexOf(subject);
			if (values[colIndex] != null && !values[colIndex].trim().isEmpty()) {
				retString = Utility.cleanString(values[colIndex], true);
			}
		}
		return retString;
	}

	private void parseMetamodel(Map<String, Object> metamodel, Owler owler, List<String> relationList, List<String> nodePropList, List<String> relPropList) {
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

	public void createRelationship(IDatabaseEngine database, String subjectNodeType, // Title
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

		startV = (Vertex) database.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT, new Object[] { subjectNodeType, instanceSubjectName });

		// upsert the object vertex
		Vertex endV = (Vertex) database.doAction(IDatabaseEngine.ACTION_TYPE.VERTEX_UPSERT, new Object[] { objectNodeType, instanceObjectName });

		// upsert the edge between them
		database.doAction(IDatabaseEngine.ACTION_TYPE.EDGE_UPSERT, new Object[] { startV, subjectNodeType, endV, objectNodeType, propHash });

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
	// Specific Tinker database inputs
	////////////////////////////////////////////
	private TINKER_DRIVER getTinkerDriverType() {
		GenRowStruct grs = this.store.getNoun(TINKER_DRIVER_TYPE);
		if (grs == null || grs.isEmpty()) {
			return TINKER_DRIVER.TG;
		}
		return TINKER_DRIVER.valueOf(grs.get(0).toString().toUpperCase());
	}
}
