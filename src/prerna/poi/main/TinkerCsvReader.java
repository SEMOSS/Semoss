package prerna.poi.main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.engine.api.IEngine;
import prerna.poi.main.helper.ImportOptions;
import prerna.util.Constants;
import prerna.util.Utility;

public class TinkerCsvReader extends AbstractCSVFileReader {

	private static final Logger logger = LogManager.getLogger(CSVReader.class.getName());

	/**
	 * Loading data into SEMOSS to create a new database
	 * 
	 * @param dbName
	 *            String grabbed from the user interface that would be used as
	 *            the name for the database
	 * @param fileNames
	 *            Absolute paths of files the user wants to load into SEMOSS,
	 *            paths are separated by ";"
	 * @param customBase
	 *            String grabbed from the user interface that is used as the URI
	 *            base for all instances
	 * @param customMap
	 * @param owlFile
	 *            String automatically generated within SEMOSS to determine the
	 *            location of the OWL file that is produced
	 * @return
	 * @throws EngineException
	 * @throws FileReaderException
	 * @throws FileWriterException
	 * @throws HeaderClassException
	 */
	public IEngine importFileWithOutConnection(ImportOptions options) throws FileNotFoundException, IOException {

		String smssLocation = options.getSMSSLocation();
		String appName = options.getDbName();
		String fileNames = options.getFileLocations();
		String customBase = options.getBaseUrl();
		String owlFile = options.getOwlFileLocation();
		String propertyFiles = options.getPropertyFiles();
		String appID = options.getEngineID();

		boolean error = false;
		logger.setLevel(Level.WARN);
		String[] files = prepareCsvReader(fileNames, customBase, owlFile, smssLocation, propertyFiles);
		try {
			openTinkerEngineWithoutConnection(appName, appID);
			for (int i = 0; i < files.length; i++) {
				try {
					String fileName = files[i];
					// open the csv file
					// and get the headers
					openCSVFile(fileName);
					// load the prop file for the CSV file
					if (propFileExist) {
						// if we have multiple files, load the correct one
						if (propFiles != null) {
							propFile = propFiles[i];
						}
						openProp(propFile);
					} else {
						rdfMap = rdfMapArr[i];
					}
					// get the user selected datatypes for each header
					preParseRdfCSVMetaData(rdfMap);
					parseMetadata();
					// processDisplayNames();
					skipRows();
					processRelationShips();
				} finally {
					closeCSVFile();
				}
			}
			// loadMetadataIntoEngine();
			createBaseRelations();
		} catch (FileNotFoundException e) {
			error = true;
			throw new FileNotFoundException(e.getMessage());
		} catch (IOException e) {
			error = true;
			throw new IOException(e.getMessage());
		} catch(Exception e) {
			error = true;
			throw new IOException(e.getMessage());
		} finally {
			if (error || autoLoad) {
				closeDB();
				closeOWL();
			} else {
				commitDB();
				
			}
		}

		return engine;
	}

	/**
	 * Load data into SEMOSS into an existing database
	 * 
	 * @param engineId
	 *            String grabbed from the user interface specifying which
	 *            database to add the data
	 * @param fileNames
	 *            Absolute paths of files the user wants to load into SEMOSS,
	 *            paths are separated by ";"
	 * @param customBase
	 *            String grabbed from the user interface that is used as the URI
	 *            base for all instances
	 * @param customMap
	 *            Absolute path specified in the CSV file that determines the
	 *            location of the prop file for the data
	 * @param owlFile
	 *            String automatically generated within SEMOSS to determine the
	 *            location of the OWL file that is produced
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void importFileWithConnection(ImportOptions options) throws FileNotFoundException, IOException {
		logger.setLevel(Level.WARN);

		String engineId = options.getEngineID();
		String fileNames = options.getFileLocations();
		String customBase = options.getBaseUrl();
		String owlFile = options.getOwlFileLocation();
		String propertyFiles = options.getPropertyFiles();

		String[] files = prepareCsvReader(fileNames, customBase, owlFile, engineId, propertyFiles);
		openEngineWithConnection(engineId);

		for (int i = 0; i < files.length; i++) {
			String fileName = files[i];
			// open the csv file
			// and get the headers
			openCSVFile(fileName);
			try {
				// load the prop file for the CSV file
				if (propFileExist) {
					// if we have multiple files, load the correct one
					if (propFiles != null) {
						propFile = propFiles[i];
					}
					openProp(propFile);
				} else {
					rdfMap = rdfMapArr[i];
				}
				// get the user selected datatypes for each header
				preParseRdfCSVMetaData(rdfMap);
				parseMetadata();
				// processDisplayNames();
				skipRows();
				processRelationShips();
			} finally {
				closeCSVFile();
			}
		}
		createBaseRelations();
		commitDB();
	}

	/**
	 * Get the data types and the csvColumnToIndex maps ready for the file load
	 * I call this preParseMetaData since this is needed for parseMetaData as it
	 * needs to know what the types are for each column to properly add into OWL
	 * 
	 * @param rdfMap
	 */
	private void preParseRdfCSVMetaData(Map<String, String> rdfMap) {
		// create the data types list
		int numCols = header.length;
		this.dataTypes = new String[numCols];
		// create a map from column name to index
		// this will be used to help speed up finding the location of values
		this.csvColumnToIndex = new Hashtable<String, Integer>();

		for (int colIndex = 1; colIndex <= numCols; colIndex++) {
			// fill in the column to index
			csvColumnToIndex.put(header[colIndex - 1], colIndex - 1);

			// fill in the data type for the column
			if (rdfMap.containsKey(colIndex + "")) {
				dataTypes[colIndex - 1] = rdfMap.get(colIndex + "");
			} else {
				// TODO: if it is not passed from the FE... lets go with string
				// for now
				dataTypes[colIndex - 1] = "STRING";
			}
		}
	}

	private void parseMetadata() {
		Set<String> concepts = new HashSet<>();
		if (rdfMap.get("RELATION") != null) {
			String relationNames = rdfMap.get("RELATION");
			StringTokenizer relationTokens = new StringTokenizer(relationNames, ";");
			relationArrayList = new ArrayList<String>();
			// process each relationship
			while (relationTokens.hasMoreElements()) {
				String relation = relationTokens.nextToken();
				// just in case the end of the prop string is empty string or
				// spaces
				if (!relation.contains("@"))
					continue;
				String subject = relation.substring(0, relation.indexOf("@"));
				String object = relation.substring(relation.lastIndexOf("@") + 1);
				String predicate = relation.substring(relation.indexOf("@") + 1, relation.lastIndexOf("@"));
				this.owler.addConcept(subject);
				concepts.add(subject);
				this.owler.addConcept(object);
				concepts.add(object);
				this.owler.addRelation(subject, object, predicate);
				relationArrayList.add(relation);
			}
		}

		if (rdfMap.get("NODE_PROP") != null) {
			String nodePropNames = rdfMap.get("NODE_PROP");
			StringTokenizer nodePropTokens = new StringTokenizer(nodePropNames, ";");
			nodePropArrayList = new ArrayList<String>();
			if (basePropURI.equals("")) {
				basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
			}
			while (nodePropTokens.hasMoreElements()) {
				String relation = nodePropTokens.nextToken();
				// in case the end of the prop string is empty string or spaces
				if (!relation.contains("%"))
					continue;
				String concept = relation.substring(0, relation.indexOf("%"));
				String property = relation.substring(relation.lastIndexOf("%") + 1);
				nodePropArrayList.add(relation);
				owler.addProp(concept, property, dataTypes[csvColumnToIndex.get(property)]);
				concepts.add(concept);
			}
		}

		if (rdfMap.get("RELATION_PROP") != null) {
			String propNames = rdfMap.get("RELATION_PROP");
			StringTokenizer propTokens = new StringTokenizer(propNames, ";");
			relPropArrayList = new ArrayList<String>();
			if (basePropURI.equals("")) {
				basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
			}
			while (propTokens.hasMoreElements()) {
				String relation = propTokens.nextToken();
				// just in case the end of the prop string is empty string or
				// spaces
				if (!relation.contains("%"))
					continue;

				relPropArrayList.add(relation);
				// TODO add relationship Property to owler
			}
		}
//		for(String c: concepts) {
//			owler.addProp(c, TinkerFrame.TINKER_NAME, dataTypes[csvColumnToIndex.get(c)]);
//		}
	}

	/**
	 * Create all the triples associated with the relationships specified in the
	 * prop file
	 * 
	 * @throws IOException
	 */
	private void processRelationShips() throws IOException {
		// get all the relation
		// overwrite this value if user specified the max rows to load

		if (rdfMap.get("END_ROW") != null) {
			maxRows = Integer.parseInt(rdfMap.get("END_ROW"));
		}
		// only start from the maxRow - the startRow
		// added -1 is because of index nature
		// the earlier rows should already have been skipped
		String[] values = null;
		while ((values = csvHelper.getNextRow()) != null && count < (maxRows)) {
			count++;
			logger.info("Process line: " + count);

			// process all relationships in row
			for (int relIndex = 0; relIndex < relationArrayList.size(); relIndex++) {
				String relation = relationArrayList.get(relIndex);
				String[] strSplit = relation.split("@");
				// get the subject and object for triple (the two indexes)
				String sub = strSplit[0].trim();
				String subject = "";
				String predicate = strSplit[1].trim();
				String obj = strSplit[2].trim();
				String object = "";

				// see if subject node URI exists in prop file
				if (rdfMap.containsKey(sub)) {
					String userSub = rdfMap.get(sub).toString();
					subject = userSub.substring(userSub.lastIndexOf("/") + 1);
				}
				// if no user specified URI, use generic URI
				else {
					if (sub.contains("+")) {
						subject = processAutoConcat(sub);
					} else {
						subject = sub;
					}
				}
				// see if object node URI exists in prop file
				if (rdfMap.containsKey(obj)) {
					String userObj = rdfMap.get(obj).toString();
					object = userObj.substring(userObj.lastIndexOf("/") + 1);
				}
				// if no user specified URI, use generic URI
				else {
					if (obj.contains("+")) {
						object = processAutoConcat(obj);
					} else {
						object = obj;
					}
				}

				String subjectValue = createInstanceValue(sub, values, csvColumnToIndex);
				String objectValue = createInstanceValue(obj, values, csvColumnToIndex);
				if (subjectValue.isEmpty() || objectValue.isEmpty()) {
					continue;
				}

				// look through all relationship properties for the specific
				// relationship
				Hashtable<String, Object> propHash = new Hashtable<String, Object>();

				for (int relPropIndex = 0; relPropIndex < relPropArrayList.size(); relPropIndex++) {
					String relProp = relPropArrayList.get(relPropIndex);
					String[] relPropSplit = relProp.split("%");
					if (relPropSplit[0].equals(relation)) {
						// loop through all properties on the relationship
						for (int i = 1; i < relPropSplit.length; i++) {
							// add the necessary triples for the relationship
							// property
							String prop = relPropSplit[i];
							String property = "";
							// see if property node URI exists in prop file
							if (rdfMap.containsKey(prop)) {
								String userProp = rdfMap.get(prop).toString();
								property = userProp.substring(userProp.lastIndexOf("/") + 1);

								// property = rdfMap.get(prop);
							}
							// if no user specified URI, use generic URI
							else {
								if (prop.contains("+")) {
									property = processAutoConcat(prop);
								} else {
									property = prop;
								}
							}
							propHash.put(property, createObject(prop, values, dataTypes, csvColumnToIndex));
						}
					}
				}
				// TODO: override this
				// default is RDF
				createRelationship(subject, object, subjectValue, objectValue, predicate, propHash);
			}

			// look through all node properties
			for (int relIndex = 0; relIndex < nodePropArrayList.size(); relIndex++) {
				Hashtable<String, Object> nodePropHash = new Hashtable<String, Object>();
				String relation = nodePropArrayList.get(relIndex);
				String[] strSplit = relation.split("%");
				// get the subject (the first index) and objects for triple
				String sub = strSplit[0].trim();
				String subject = "";
				// see if subject node URI exists in prop file
				if (rdfMap.containsKey(sub)) {
					String userSub = rdfMap.get(sub).toString();
					subject = userSub.substring(userSub.lastIndexOf("/") + 1);

					// subject = rdfMap.get(sub);
				}
				// if no user specified URI, use generic URI
				else {
					if (sub.contains("+")) {
						subject = processAutoConcat(sub);
					} else {
						subject = sub;
					}
				}
				String subjectValue = createInstanceValue(sub, values, csvColumnToIndex);
				// loop through all properties on the node
				for (int i = 1; i < strSplit.length; i++) {
					String prop = strSplit[i].trim();
					String property = "";
					// see if property node URI exists in prop file
					if (rdfMap.containsKey(prop)) {
						String userProp = rdfMap.get(prop).toString();
						property = userProp.substring(userProp.lastIndexOf("/") + 1);

						// property = rdfMap.get(prop);
					}
					// if no user specified URI, use generic URI
					else {
						if (prop.contains("+")) {
							property = processAutoConcat(prop);
						} else {
							property = prop;
						}
					}
					Object propObj = createObject(prop, values, dataTypes, csvColumnToIndex);
					if (propObj == null || propObj.toString().isEmpty()) {
						continue;
					}
					nodePropHash.put(property, propObj);
				}
				// TODO: override this
				// default is RDF
				addNodeProperties(subject, subjectValue, nodePropHash);
			}
		}
		System.out.println("FINAL COUNT " + count);
	}

	public void addNodeProperties(String nodeType, String instanceName, Hashtable<String, Object> propHash) {
		// create the node in case its not in a relationship
		instanceName = Utility.cleanString(instanceName, true);
		nodeType = Utility.cleanString(nodeType, true);
		String semossBaseURI = owler.addConcept(nodeType);
		String instanceBaseURI = getInstanceURI(nodeType);
		String subjectNodeURI = instanceBaseURI + "/" + instanceName;
		Vertex vert = (Vertex) engine.doAction(IEngine.ACTION_TYPE.VERTEX_UPSERT, new Object[] {nodeType, instanceName});
		Set<String> vertProps = vert.keys();
		for (String key : propHash.keySet()) {
			if (!vertProps.contains(key)) {
				vert.property(key, propHash.get(key));
			}
		}

	}

	public void createRelationship(String subjectNodeType, // Title
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
			startV = (Vertex) engine.doAction(IEngine.ACTION_TYPE.VERTEX_UPSERT, new Object[] { subjectNodeType, instanceSubjectName });
		} catch(Exception e) {
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
	 * @param colNameToIndex
	 *            Map containing the header names to index within the values
	 *            array
	 * @return The return is the value for the instance
	 */
	private String createInstanceValue(String subject, String[] values, Map<String, Integer> colNameToIndex) {
		String retString = "";
		// if node is a concatenation
		if (subject.contains("+")) {
			String elements[] = subject.split("\\+");
			for (int i = 0; i < elements.length; i++) {
				String subjectElement = elements[i];
				int colIndex = colNameToIndex.get(subjectElement);
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
			int colIndex = colNameToIndex.get(subject);
			if (values[colIndex] != null && !values[colIndex].trim().isEmpty()) {
				retString = Utility.cleanString(values[colIndex], true);
			}
		}
		return retString;
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
	 * @param dataTypes
	 *            The string[] containing the data type for each column in the
	 *            values array
	 * @param colNameToIndex
	 *            Map containing the column name to index in values[] for fast
	 *            retrieval of data
	 * @return The object in the correct data format
	 */
	private Object createObject(String object, String[] values, String[] dataTypes,
			Map<String, Integer> colNameToIndex) {
		// if it contains a plus sign, it is a concatenation
		if (object.contains("+")) {
			StringBuilder strBuilder = new StringBuilder();
			String[] objList = object.split("\\+");
			for (int i = 0; i < objList.length; i++) {
				strBuilder.append(values[colNameToIndex.get(objList[i])]);
			}
			return Utility.cleanString(strBuilder.toString(), true);
		}

		// here we need to grab the value and cast it based on the type
		Object retObj = null;
		int colIndex = colNameToIndex.get(object);

		String type = dataTypes[colIndex];
		String strVal = values[colIndex];
		if (type.equals("NUMBER")) {
			retObj = Utility.getDouble(strVal);
		} else if (type.equals("DATE")) {
			retObj = Utility.getDateAsDateObj(strVal);
		} else {
			retObj = strVal;
		}

		return retObj;
	}
}