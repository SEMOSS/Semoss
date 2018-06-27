package prerna.sablecc2.reactor.app.upload.rdf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class RdfCsvUploadReactor extends AbstractReactor {
	private static final String CLASS_NAME = RdfCsvUploadReactor.class.getName();
	protected final String DATA_TYPE_MAP = ReactorKeysEnum.DATA_TYPE_MAP.getKey();
	protected final String NEW_HEADERS = ReactorKeysEnum.NEW_HEADER_NAMES.getKey();
	protected final String ADDITIONAL_DATA_TYPES = ReactorKeysEnum.ADDITIONAL_DATA_TYPES.getKey();

	public RdfCsvUploadReactor() {
		this.keysToGet = new String[] { UploadInputUtility.APP, UploadInputUtility.FILE_PATH,
				UploadInputUtility.DELIMITER, DATA_TYPE_MAP, NEW_HEADERS, ReactorKeysEnum.METAMODEL.getKey(), ADDITIONAL_DATA_TYPES,
				UploadInputUtility.ADD_TO_EXISTING };
	}

	@Override
	public NounMetadata execute() {
		final String appIdOrName = UploadInputUtility.getAppName(this.store);
		final boolean existing = UploadInputUtility.getExisting(this.store);
		final String filePath = UploadInputUtility.getFilePath(this.store);
		final File file = new File(filePath);
		if (!file.exists()) {
			throw new IllegalArgumentException("Could not find the file path specified");
		}
		String returnId = null;
		if (existing) {
			returnId = addToExistingApp(appIdOrName, filePath);
		} else {
			returnId = generateNewApp(appIdOrName, filePath);
		}
		return new NounMetadata(returnId, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	private String generateNewApp(String newAppName, String filePath) {
		Logger logger = getLogger(CLASS_NAME);
		String newAppId = UUID.randomUUID().toString();
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, String> dataTypesMap = getDataTypeMap();
		Map<String, String> newHeaders = getNewHeaders();
		
		// start by validation
		logger.info("Start validating app");
		try {
			UploadUtilities.validateApp(newAppName);
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("Done validating app");

		logger.info("1. Start generating app folder");
		UploadUtilities.generateAppFolder(newAppId, newAppName);
		logger.info("1. Complete");

		logger.info("2. Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(newAppId, newAppName);
		logger.info("2. Complete");

		logger.info("3. Create properties file for database...");
		File tempSmss = null;
		try {
			tempSmss = UploadUtilities.createTemporaryRdfSmss(newAppId, newAppName, owlFile);
			DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("3. Complete");

		logger.info("4. Create  database store...");
		BigDataEngine engine = new BigDataEngine();
		engine.setEngineId(newAppId);
		engine.setEngineName(newAppName);
		engine.openDB(tempSmss.getAbsolutePath());
		String semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String typeOf = RDF.TYPE.stringValue();
		String obj = Constants.CLASS_URI;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, typeOf, obj, true});
		sub =  semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		obj = Constants.DEFAULT_PROPERTY_URI;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, typeOf, obj, true});
		logger.info("4. Complete...");

		logger.info("5. Start loading data..");
		//TODO clean up
		boolean error = false;
		logger.setLevel(Level.WARN);
		CSVFileHelper helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), engine.getEngineType());
		try {
			// open the csv file
			// and get the headers
			logger.info("5. Start loading data..");

			// load the prop file for the CSV file
			// if(propFileExist){
			// // if we have multiple files, load the correct one
			// if(propFiles != null) {
			// propFile = propFiles[i];
			// }
			// openProp(propFile);
			// } else {
			// rdfMap = rdfMapArr[i];
			// }
			// get the user selected datatypes for each header
			logger.info("Parsing file metadata...");
			Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(helper, dataTypesMap, getAdditionalTypes());
			String[] headers = (String[]) headerTypesArr[0];
			SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
			String[] additionalTypes = (String[]) headerTypesArr[2];
			logger.info("Done parsing  file metadata");

			// parse metamodel
			List<String> nodePropList = new ArrayList<String>();
			List<String> relationList = new ArrayList<String>();
			List<String> relPropList = new ArrayList<String>();
			parseMetadata(getMetamodel(), nodePropList, relationList, relPropList);
			//TODO
//			skipRows();
			processRelationShips(engine, owler,helper, nodePropList, relationList, relPropList, Arrays.asList(headers), types);

			loadMetadataIntoEngine(engine, owler);
			owler.commit();
			try {
				owler.export();
			} catch (IOException ex) {
				ex.printStackTrace();
				throw new IOException("Unable to export OWL file...");
			}
//			RDFEngineCreationHelper.insertSelectConceptsAsInsights(engine, owler.getConceptualNodes());
		} catch (FileNotFoundException e) {
			error = true;
//			throw new FileNotFoundException(e.getMessage());
		} catch (IOException e) {
			error = true;
//			throw new IOException(e.getMessage());
		} finally {
			// if (error || autoLoad) {
			// closeDB();
			// closeOWL();
			// } else {
			logger.warn("Committing....");
			// commit the created engine
			engine.commit();
			engine.infer();

			// also commit the created insights rdbms engine
			engine.getInsightDatabase().commit();
			// }
		}

		logger.info("7. Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppId, newAppName);
		UploadUtilities.addExploreInstanceInsight(newAppId, insightDatabase);
		engine.setInsightDatabase(insightDatabase);
		RDBMSEngineCreationHelper.insertAllTablesAsInsights(engine);
		logger.info("7. Complete");

		logger.info("8. Process app metadata to allow for traversing across apps	");
		try {
			UploadUtilities.updateMetadata(newAppId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("8. Complete");

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

	private String addToExistingApp(String appIdOrName, String filePath) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private void parseMetadata(Map<String, Object> metamodel, List<String> nodePropList, List<String> relationList, List<String> relPropList) {
		if (metamodel.get(Constants.NODE_PROP) != null) {
			if (metamodel.get(Constants.NODE_PROP) != null) {
				Map<String, Object> nodeProps = (Map<String, Object>) metamodel.get(Constants.NODE_PROP);
				// if(basePropURI.equals("")){
				// basePropURI = semossURI + "/" +
				// Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
				// }
				for (String concept : nodeProps.keySet()) {
					List<String> conceptProps = (List<String>) nodeProps.get(concept);
					for (String property : conceptProps) {
						String relation = concept + "%" + property;
						nodePropList.add(relation);
					}
				}

			}
		}
		if (metamodel.get(Constants.RELATION) != null) {
			List<Map<String, Object>> edgeList = (List<Map<String, Object>>) metamodel.get(Constants.RELATION);
			// process each relationship
			for (Map relMap : edgeList) {
				String subject = (String) relMap.get(Constants.FROM_TABLE);
				String object = (String) relMap.get(Constants.TO_TABLE);
				String predicate = (String) relMap.get(Constants.REL_NAME);
				String relation = subject + "@" + predicate + "@" + object;
				relationList.add(relation);
			}
		}
		if (metamodel.get(Constants.RELATION_PROP) != null) {
			Map<String, Object> relPropMap = (Map<String, Object>) metamodel.get(Constants.RELATION_PROP);
			// if(basePropURI.equals("")){
			// basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS
			// + "/" + CONTAINS;
			// }
			for (String relName : relPropMap.keySet()) {
				List<String> relProps = (List<String>) relPropMap.get(relName);
				for (String property : relProps) {
					String relation = relName + "%" + property;
					relPropList.add(relation);
				}
			}
		}
	}
	
	protected void loadMetadataIntoEngine(IEngine engine, OWLER owler) {
		Hashtable<String, String> hash = owler.getConceptHash();
		String object = OWLER.SEMOSS_URI + OWLER.DEFAULT_NODE_CLASS;
		for(String concept : hash.keySet()) {
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{hash.get(concept), RDFS.SUBCLASSOF + "", object, true});
		}
		hash = owler.getRelationHash();
		object = OWLER.SEMOSS_URI + OWLER.DEFAULT_RELATION_CLASS;
		for(String relation : hash.keySet()) {
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{hash.get(relation), RDFS.SUBPROPERTYOF + "", object, true});
		}
		hash = owler.getPropHash();
		object = OWLER.SEMOSS_URI + OWLER.DEFAULT_PROP_CLASS;
		for(String prop : hash.keySet()) {
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{hash.get(prop), RDF.TYPE + "", object, true});
		}
	}
	
	///////////////////////////////////////////////////////////////////
	//////////////Methods to insert data///////////////////////////////
	///////////////////////////////////////////////////////////////////
	/**
	 * Create all the triples associated with the relationships specified in the prop file
	 * @throws IOException 
	 */
	private void processRelationShips(IEngine engine, OWLER owler, CSVFileHelper helper, List<String> nodePropList, List<String> relationList, List<String> relPropList, List<String> headers, SemossDataType[] dataTypes) throws IOException 
	{
		//TODO user subjects
		Map<String, Object> rdfMap = new HashMap<>();
		// get all the relation
		// overwrite this value if user specified the max rows to load
//		if (rdfMap.get("END_ROW") != null)
//		{
//			maxRows =  Integer.parseInt(rdfMap.get("END_ROW"));
//		}
		// only start from the maxRow - the startRow
		// added -1 is because of index nature
		// the earlier rows should already have been skipped
		String[] values = null;
		int count = 0;
		int startRow = 2;
		int maxRows = 2_000_000_000;
		Logger logger = getLogger(CLASS_NAME);
		while( (values = helper.getNextRow()) != null && count<(maxRows))
		{
			count++;
			logger.info("Process line: " +count);

			// process all relationships in row
			for(int relIndex = 0; relIndex < relationList.size(); relIndex++)
			{
				String relation = relationList.get(relIndex);
				String[] strSplit = relation.split("@");
				// get the subject and object for triple (the two indexes)
				String sub = strSplit[0].trim();
				String subject = "";
				String predicate = strSplit[1].trim();
				String obj = strSplit[2].trim();
				String object = "";

				// see if subject node URI exists in prop file
				if(rdfMap.containsKey(sub))
				{
					String userSub = rdfMap.get(sub).toString(); 
					subject = userSub.substring(userSub.lastIndexOf("/")+1);
				}
				// if no user specified URI, use generic URI
				else
				{
					if(sub.contains("+"))
					{
						subject = processAutoConcat(sub);
					}
					else
					{
						subject = sub;
					}
				}
				// see if object node URI exists in prop file
				if(rdfMap.containsKey(obj))
				{
					String userObj = rdfMap.get(obj).toString(); 
					object = userObj.substring(userObj.lastIndexOf("/")+1);
				}
				// if no user specified URI, use generic URI
				else
				{
					if(obj.contains("+"))
					{
						object = processAutoConcat(obj);
					}
					else
					{
						object = obj;
					}
				}

				String subjectValue = createInstanceValue(sub, values, headers);
				String objectValue = createInstanceValue(obj, values, headers);
				if (subjectValue.isEmpty() || objectValue.isEmpty())
				{
					continue;
				}

				// look through all relationship properties for the specific relationship
				Hashtable<String, Object> propHash = new Hashtable<String, Object>();

				for(int relPropIndex = 0; relPropIndex < relPropList.size(); relPropIndex++)
				{
					String relProp = relPropList.get(relPropIndex);
					String[] relPropSplit = relProp.split("%");
					if(relPropSplit[0].equals(relation))
					{
						// loop through all properties on the relationship
						for(int i = 1; i < relPropSplit.length; i++)
						{			
							// add the necessary triples for the relationship property
							String prop = relPropSplit[i];
							String property = "";
							// see if property node URI exists in prop file
							if(rdfMap.containsKey(prop))
							{
								String userProp = rdfMap.get(prop).toString(); 
								property = userProp.substring(userProp.lastIndexOf("/")+1);

								//property = rdfMap.get(prop);
							}
							// if no user specified URI, use generic URI
							else
							{
								if(prop.contains("+"))
								{
									property = processAutoConcat(prop);
								}
								else
								{
									property = prop;
								}
							}
							propHash.put(property, createObject(prop, values, dataTypes, headers));
						}
					}
				}
				createRelationship(engine, owler, subject, object, subjectValue, objectValue, predicate, propHash);
			}

			// look through all node properties
			for(int relIndex = 0;relIndex<nodePropList.size();relIndex++)
			{
				Hashtable<String, Object> nodePropHash = new Hashtable<String, Object>();
				String relation = nodePropList.get(relIndex);
				String[] strSplit = relation.split("%");
				// get the subject (the first index) and objects for triple
				String sub = strSplit[0].trim();
				String subject = "";
				// see if subject node URI exists in prop file
				if(rdfMap.containsKey(sub))
				{
					String userSub = rdfMap.get(sub).toString(); 
					subject = userSub.substring(userSub.lastIndexOf("/")+1);

					//subject = rdfMap.get(sub);
				}
				// if no user specified URI, use generic URI
				else
				{	
					if(sub.contains("+"))
					{
						subject = processAutoConcat(sub);
					}
					else
					{
						subject = sub;
					}
				}
				String subjectValue = createInstanceValue(sub, values, headers);
				// loop through all properties on the node
				for(int i = 1; i < strSplit.length; i++)
				{
					String prop = strSplit[i].trim();
					String property = "";
					// see if property node URI exists in prop file
					if(rdfMap.containsKey(prop))
					{
						String userProp = rdfMap.get(prop).toString(); 
						property = userProp.substring(userProp.lastIndexOf("/")+1);

						//property = rdfMap.get(prop);
					}
					// if no user specified URI, use generic URI
					else
					{
						if(prop.contains("+"))
						{
							property = processAutoConcat(prop);
						}
						else
						{
							property = prop;
						}
					}
					Object propObj = createObject(prop, values, dataTypes, headers);
					if (propObj == null || propObj.toString().isEmpty()) {
						continue;
					}
					nodePropHash.put(property, propObj);
				}
				addNodeProperties(engine, owler, subject, subjectValue, nodePropHash);
			}
		}
		System.out.println("FINAL COUNT " + count);
	}
	
	public void addNodeProperties(IEngine engine, OWLER owler, String nodeType, String instanceName, Hashtable<String, Object> propHash) {
		//create the node in case its not in a relationship
		instanceName = Utility.cleanString(instanceName, true);
		nodeType = Utility.cleanString(nodeType, true); 
		String semossBaseURI = owler.addConcept(nodeType);
		String instanceBaseURI = getInstanceURI(nodeType);
		String subjectNodeURI = instanceBaseURI + "/" + instanceName;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, RDF.TYPE, semossBaseURI, true});
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, RDFS.LABEL, instanceName, false});

		addProperties(engine, owler, nodeType, subjectNodeURI, propHash);
	}
	
	/**
	 * Gets the instance value for a given subject.  The subject can be a concatenation. Note that we do 
	 * not care about the data type for this since a URI is always a string
	 * @param subject					The subject type (i.e. concept, or header name) to get the instance value for
	 * @param values					String[] containing the values for the row
	 * @param colNameToIndex			Map containing the header names to index within the values array
	 * @return							The return is the value for the instance
	 */
	private String createInstanceValue(String subject, String[] values, List<String> headers)
	{
		String retString ="";
		// if node is a concatenation
		if(subject.contains("+")) 
		{
			String elements[] = subject.split("\\+");
			for (int i = 0; i<elements.length; i++)
			{
				String subjectElement = elements[i];
				int colIndex = headers.indexOf(subjectElement);
				if(values[colIndex] != null && !values[colIndex].toString().trim().isEmpty())
				{
					String value = values[colIndex] + "";
					value = Utility.cleanString(value, true);

					retString = retString  + value + "-";
				}
				else
				{
					retString = retString  + "null-";
				}
			}
			// a - will show up at the end of this and we need to get rid of that
			if(!retString.equals(""))
				retString = retString.substring(0,retString.length()-1);
		}
		else
		{
			// if the value is not empty, get the correct value to return
			int colIndex = headers.indexOf(subject);
			if(values[colIndex] != null && !values[colIndex].trim().isEmpty()) {
				retString = Utility.cleanString(values[colIndex], true);
			}
		}
		return retString;
	}
	
	/**
	 * Gets the properly formatted object from the string[] values object
	 * Also handles if the column is a concatenation
	 * @param object					The column to get the correct data type for - can be a concatenation
	 * @param values					The string[] containing the values for the row
	 * @param dataTypes					The string[] containing the data type for each column in the values array
	 * @param colNameToIndex			Map containing the column name to index in values[] for fast retrieval of data
	 * @return							The object in the correct data format
	 */
	private Object createObject(String object, String[] values, SemossDataType[] dataTypes, List<String> headers )
	{
		// if it contains a plus sign, it is a concatenation
		if(object.contains("+")) {
			StringBuilder strBuilder = new StringBuilder();
			String[] objList = object.split("\\+");
			for(int i = 0; i < objList.length; i++){
				strBuilder.append(values[headers.indexOf(objList[i])]); 
			}
			return Utility.cleanString(strBuilder.toString(), true);
		}

		// here we need to grab the value and cast it based on the type
		Object retObj = null;
		int colIndex = headers.indexOf(object);

		SemossDataType type = dataTypes[colIndex];
		String strVal = values[colIndex];
		if(type.equals("INT")) {
			retObj = Utility.getInteger(strVal);
		} else if(type.equals("NUMBER") || type.equals("DOUBLE")) {
			retObj = Utility.getDouble(strVal);
		} else if(type.equals("DATE")) {
			Long dTime = SemossDate.getTimeForDate(strVal);
			if(dTime != null) {
				retObj = new SemossDate(dTime, "yyyy-MM-dd");
			}
		} else if(type.equals("TIMESTAMP")) {
			Long dTime = SemossDate.getTimeForTimestamp(strVal);
			if(dTime != null) {
				retObj = new SemossDate(dTime, "yyyy-MM-dd HH:mm:ss");
			}
		} else {
			retObj = strVal;
		}

		return retObj;
	}
	
	private String processAutoConcat(String input)
	{
		String[] split = input.split("\\+");
		String output = "";
		for (int i=0;i<split.length;i++)
		{
			output = output+split[i].trim();
		}
		return Utility.cleanString(output, true);
	}
	
	
	
	
	/**
	 * Create and add all triples associated with relationship tabs
	 * @param subjectNodeType					String containing the subject node type
	 * @param objectNodeType					String containing the object node type
	 * @param instanceSubjectName				String containing the name of the subject instance
	 * @param instanceObjectName				String containing the name of the object instance
	 * @param relName							String containing the name of the relationship between the subject and object
	 * @param propHash							Hashtable that contains all properties
	 */
	public void createRelationship(IEngine engine, OWLER owler, String subjectNodeType, String objectNodeType, String instanceSubjectName,
			String instanceObjectName, String relName, Hashtable<String, Object> propHash) {
		subjectNodeType = Utility.cleanString(subjectNodeType, true);
		objectNodeType = Utility.cleanString(objectNodeType, true);

		instanceSubjectName = Utility.cleanString(instanceSubjectName, true);
		instanceObjectName = Utility.cleanString(instanceObjectName, true);

		// get base URIs for subject node at instance and semoss level
		String subjectSemossBaseURI = owler.addConcept(subjectNodeType);
		String subjectInstanceBaseURI = getInstanceURI(subjectNodeType);

		// get base URIs for object node at instance and semoss level
		String objectSemossBaseURI = owler.addConcept(objectNodeType);
		String objectInstanceBaseURI = getInstanceURI(objectNodeType);

		// create the full URI for the subject instance
		// add type and label triples to database
		String subjectNodeURI = subjectInstanceBaseURI + "/" + instanceSubjectName; 
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { subjectNodeURI, RDF.TYPE, subjectSemossBaseURI, true });
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { subjectNodeURI, RDFS.LABEL, instanceSubjectName, false });

		// create the full URI for the object instance
		// add type and label triples to database
		String objectNodeURI = objectInstanceBaseURI + "/" + instanceObjectName; 
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { objectNodeURI, RDF.TYPE, objectSemossBaseURI, true });
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT,new Object[] { objectNodeURI, RDFS.LABEL, instanceObjectName, false });

		// generate URIs for the relationship
		relName = Utility.cleanPredicateString(relName);
		String relSemossBaseURI = owler.addRelation(subjectNodeType, objectNodeType, relName);
		String relInstanceBaseURI = getRelationURI(relName);

		// create instance value of relationship and add instance relationship,
		// subproperty, and label triples
		String instanceRelURI = relInstanceBaseURI + "/" + instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceRelURI, RDFS.SUBPROPERTYOF, relSemossBaseURI, true });
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceRelURI, RDFS.LABEL, 
				instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName, false });
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { subjectNodeURI, instanceRelURI, objectNodeURI, true });

		addProperties(engine, owler, "", instanceRelURI, propHash);
	}
	

	public void addProperties(IEngine engine, OWLER owler, String subjectNodeType, String instanceURI, Hashtable<String, Object> propHash) {
		Logger logger = getLogger(CLASS_NAME);
		// add all properties
		Enumeration<String> propKeys = propHash.keys();
		String basePropURI = getBasePropURI();
		
		// add property triple based on data type of property
		while (propKeys.hasMoreElements()) {
			String key = propKeys.nextElement().toString();
			String propURI = basePropURI + "/" + Utility.cleanString(key, true);
			// logger.info("Processing Property " + key + " for " + instanceURI);
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { propURI, RDF.TYPE, basePropURI, true });
			if (propHash.get(key) instanceof Number) {
				Double value = ((Number) propHash.get(key)).doubleValue();
				// logger.info("Processing Double value " + value);
				engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, value.doubleValue(), false });
				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owler.addProp(subjectNodeType, key, "DOUBLE");
				}
			} else if (propHash.get(key) instanceof Date) {
				Date value = (Date) propHash.get(key);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(value);
				Date dateFormatted;
				try {
					dateFormatted = df.parse(date);
				} catch (ParseException e) {
					logger.error("ERROR: could not parse date: " + date);
					continue;
				}
				// logger.info("Processing Date value " + dateFormatted);
				engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, dateFormatted, false });
				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owler.addProp(subjectNodeType, key, "DATE");
				}
			} else if (propHash.get(key) instanceof Boolean) {
				Boolean value = (Boolean) propHash.get(key);
				// logger.info("Processing Boolean value " + value);
				engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, value.booleanValue(), false });
				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owler.addProp(subjectNodeType, key, "BOOLEAN");
				}
			} else {
				String value = propHash.get(key).toString();
				if (value.equals(Constants.PROCESS_CURRENT_DATE)) {
					// logger.info("Processing Current Date Property");
					insertCurrentDate(engine, propURI, basePropURI, instanceURI);
				} else if (value.equals(Constants.PROCESS_CURRENT_USER)) {
					// logger.info("Processing Current User Property");
					insertCurrentUser(engine, propURI, basePropURI, instanceURI);
				} else {
					String cleanValue = Utility.cleanString(value, true, false, true);
					// logger.info("Processing String value " + cleanValue);
					engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, cleanValue, false });
				}
				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owler.addProp(subjectNodeType, key, "STRING");
				}
			}
		}
	}
	
	/**
	 * Insert the current date as a property onto a node if property is "PROCESS_CURRENT_DATE"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 */
	private void insertCurrentDate(IEngine engine, String propInstanceURI, String basePropURI, String subjectNodeURI) {
		Logger logger = getLogger(CLASS_NAME);
		Date dValue = new Date();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String date = df.format(dValue);
		Date dateFormatted;
		try {
			dateFormatted = df.parse(date);
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propInstanceURI, RDF.TYPE, basePropURI, true});
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, propInstanceURI, dateFormatted, false});
		} catch (ParseException e) {
			logger.error("ERROR: could not parse date: " + date);
		}
	}
	
	/**
	 * Insert the current user as a property onto a node if property is "PROCESS_CURRENT_USER"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 */
	private void insertCurrentUser(IEngine engine, String propURI, String basePropURI, String subjectNodeURI) {
		String cleanValue = System.getProperty("user.name");
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propURI, RDF.TYPE, basePropURI, true});
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, propURI, cleanValue, false});
	}
	
	public String getInstanceURI(String nodeType) {
		String customBaseURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		//TODO user input
		return customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ nodeType;
	}
	
	public String getRelationURI(String relName) {
		String customBaseURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		//TODO user input
		return  customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
	}

	public String getBasePropURI() {
		String semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		return semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + "Contains";
	}
	

	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////
	/*
	 * Getters from noun store
	 */

	protected Map<String, String> getDataTypeMap() {
		GenRowStruct grs = this.store.getNoun(DATA_TYPE_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	protected Map<String, String> getNewHeaders() {
		GenRowStruct grs = this.store.getNoun(NEW_HEADERS);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	protected Map<String, String> getAdditionalTypes() {
		GenRowStruct grs = this.store.getNoun(ADDITIONAL_DATA_TYPES);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	private Map<String, Object> getMetamodel() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.METAMODEL.getKey());
		if (grs == null || grs.isEmpty()) {
			return null;
		}

		return (Map<String, Object>) grs.get(0);
	}
	
	
	
	
	
}
