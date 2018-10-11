package prerna.sablecc2.reactor.app.upload.rdf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.PushAppRunner;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.poi.main.RDFEngineCreationHelper;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class RdfCsvUploadReactor extends AbstractRdfUpload {
	private static final String CLASS_NAME = RdfCsvUploadReactor.class.getName();

	public RdfCsvUploadReactor() {
		this.keysToGet = new String[] { UploadInputUtility.APP, UploadInputUtility.FILE_PATH,
				UploadInputUtility.DELIMITER, UploadInputUtility.DATA_TYPE_MAP, UploadInputUtility.NEW_HEADERS,
				UploadInputUtility.METAMODEL, UploadInputUtility.PROP_FILE, UploadInputUtility.ADD_TO_EXISTING,
				UploadInputUtility.START_ROW, UploadInputUtility.END_ROW, UploadInputUtility.ADDITIONAL_DATA_TYPES,
				UploadInputUtility.CUSTOM_BASE_URI };
	}

	@Override
	public NounMetadata execute() {
		final String appIdOrName = UploadInputUtility.getAppName(this.store);
		final boolean existing = UploadInputUtility.getExisting(this.store);
		final String filePath = UploadInputUtility.getFilePath(this.store);
		final File file = new File(filePath);
		// check security
		User user = null;
		boolean security = AbstractSecurityUtils.securityEnabled();
		if(security) {
			user = this.insight.getUser();
			if(user == null) {
				NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a database", PixelDataType.CONST_STRING, 
						PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		if (!file.exists()) {
			throw new IllegalArgumentException("Could not find the file path specified");
		}
		String appId = null;
		if (existing) {
			if(security) {
				if(!SecurityQueryUtils.userCanEditEngine(user, appIdOrName)) {
					NounMetadata noun = new NounMetadata("User does not have sufficient priviledges to update the database", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
					SemossPixelException err = new SemossPixelException(noun);
					err.setContinueThreadOfExecution(false);
					throw err;
				}
			}
			appId = addToExistingApp(appIdOrName, filePath);
		} else {
			appId = generateNewApp(appIdOrName, filePath);
		}
		// even if no security, just add user as engine owner
		if(user != null) {
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider ap : logins) {
				SecurityUpdateUtils.addEngineOwner(appId, user.getAccessToken(ap).getId());
			}
		}
		
		ClusterUtil.reactorPushApp(appId);
		
		Map<String, Object> retMap = UploadUtilities.getAppReturnData(this.insight.getUser(),appId);
		return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	private String generateNewApp(String newAppName, String filePath) {
		Logger logger = getLogger(CLASS_NAME);
		String newAppId = UUID.randomUUID().toString();
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, String> newHeaders = UploadInputUtility.getNewCsvHeaders(this.store);
		Map<String, String> additionalDataTypes = UploadInputUtility.getAdditionalCsvDataTypes(this.store);

		int stepCounter = 1;
		logger.info(stepCounter + ".Start validating app");
		try {
			UploadUtilities.validateApp(newAppName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info(stepCounter + ".Done validating app");
		stepCounter++;

		logger.info(stepCounter + ". Start generating app folder");
		UploadUtilities.generateAppFolder(newAppId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create metadata for app...");
		File owlFile = UploadUtilities.generateOwlFile(newAppId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create properties file for app...");
		File tempSmss = null;
		try {
			tempSmss = UploadUtilities.createTemporaryRdfSmss(newAppId, newAppName, owlFile);
			DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE,
					tempSmss.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		// get metamodel
		Map<String, Object> metamodelProps = UploadInputUtility.getMetamodelProps(this.store);
		Map<String, String> dataTypesMap = null;
		if (metamodelProps != null) {
			dataTypesMap = (Map<String, String>) metamodelProps.get(Constants.DATA_TYPES);
		}

		/*
		 * Load data into rdf engine
		 */
		logger.info(stepCounter + ". Create  database store...");
		BigDataEngine engine = new BigDataEngine();
		engine.setEngineId(newAppId);
		engine.setEngineName(newAppName);
		engine.openDB(tempSmss.getAbsolutePath());
		String semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String typeOf = RDF.TYPE.stringValue();
		String obj = Constants.CLASS_URI;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { sub, typeOf, obj, true });
		sub = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		obj = Constants.DEFAULT_PROPERTY_URI;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { sub, typeOf, obj, true });
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start loading data..");
		boolean error = false;
		logger.setLevel(Level.WARN);
		CSVFileHelper helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), engine.getEngineType());
		owler.addCustomBaseURI(UploadInputUtility.getCustomBaseURI(this.store));
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(helper, dataTypesMap, additionalDataTypes);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		processRelationships(engine, owler, helper, Arrays.asList(headers), types, metamodelProps);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Commit app metadata...");
		loadMetadataIntoEngine(engine, owler);
		owler.commit();
		try {
			owler.export();
		} catch (IOException ex) {
			ex.printStackTrace();
			// throw new IOException("Unable to export OWL file...");
		}
		// commit the created engine
		engine.setOWL(owler.getOwlPath());
		engine.commit();
		engine.infer();
		logger.info(stepCounter + ". Complete...");
		stepCounter++;

		/*
		 * Back to normal upload app stuff
		 */

		// and rename .temp to .smss
		File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempSmss, smssFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		tempSmss.delete();
		engine.setPropFile(smssFile.getAbsolutePath());
		UploadUtilities.updateDIHelper(newAppId, newAppName, engine, smssFile);

		logger.info(stepCounter + ". Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppId, newAppName);
		UploadUtilities.addExploreInstanceInsight(newAppId, insightDatabase);
		engine.setInsightDatabase(insightDatabase);
		RDFEngineCreationHelper.insertSelectConceptsAsInsights(engine, owler.getConceptualNodes());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Process app metadata to allow for traversing across apps	");
		try {
			UploadUtilities.updateMetadata(newAppId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		
		logger.info(stepCounter + ". Save csv metamodel prop file	");
		UploadUtilities.createPropFile(newAppId, newAppName, filePath, metamodelProps);
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		
		return newAppId;
	}

	private String addToExistingApp(String appIdOrName, String filePath) {
		// get existing app
		Logger logger = getLogger(CLASS_NAME);
		int stepCounter = 1;
		logger.info(stepCounter + ". Get existing app..");
		appIdOrName = MasterDatabaseUtility.testEngineIdIfAlias(appIdOrName);
		if(!(Utility.getEngine(appIdOrName) instanceof BigDataEngine)) {
			throw new IllegalArgumentException("Invalid engine type");
		}
		BigDataEngine engine = (BigDataEngine) Utility.getEngine(appIdOrName);
		String appID = engine.getEngineId();
		logger.info(stepCounter + ". Done..");
		stepCounter++;
		
		logger.info(stepCounter + "Get app upload input...");
		boolean error = false;
		logger.setLevel(Level.WARN);
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, String> newHeaders = UploadInputUtility.getNewCsvHeaders(this.store);
		Map<String, String> additionalDataTypes = UploadInputUtility.getAdditionalCsvDataTypes(this.store);
		Map<String, Object> metamodelProps = UploadInputUtility.getMetamodelProps(this.store);
		Map<String, String> dataTypesMap = null;
		if (metamodelProps != null) {
			dataTypesMap = (Map<String, String>) metamodelProps.get(Constants.DATA_TYPES);
		}
		logger.info(stepCounter + "Done...");
		stepCounter++;
		
		logger.info(stepCounter + "Parsing file metadata...");
		CSVFileHelper helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		// get the user selected datatypes for each header
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(helper, dataTypesMap, additionalDataTypes);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		
		
		logger.info(stepCounter + ". Start loading data..");
		OWLER owler = new OWLER(engine, engine.getOWL());
		processRelationships(engine, owler, helper, Arrays.asList(headers), types, metamodelProps);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.warn(stepCounter + "Committing app metadata....");
		loadMetadataIntoEngine(engine, owler);
		owler.commit();
		try {
			owler.export();
		} catch (IOException ex) {
			ex.printStackTrace();
			// throw new IOException("Unable to export OWL file...");
		}
		// commit the created engine
		engine.commit();
		engine.infer();
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		logger.info(stepCounter + ". Start generating default app insights");
		RDFEngineCreationHelper.insertNewSelectConceptsAsInsights(engine, owler.getConceptualNodes());
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		
		logger.info(stepCounter + ". Process app metadata to allow for traversing across apps	");
		try {
			UploadUtilities.updateMetadata(appID);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info(stepCounter + ". Complete");
		
		logger.info(stepCounter + ". Save csv metamodel prop file	");
		UploadUtilities.createPropFile(appID, engine.getEngineName(), filePath, metamodelProps);
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		
		return appID;
	}
	
	private void parseMetamodel(Map<String, Object> metamodel, List<String> nodePropList, List<String> relationList, List<String> relPropList) {
		// get node properties
		if (metamodel.get(Constants.NODE_PROP) != null) {
			if (metamodel.get(Constants.NODE_PROP) != null) {
				Map<String, Object> nodeProps = (Map<String, Object>) metamodel.get(Constants.NODE_PROP);
				for (String concept : nodeProps.keySet()) {
					List<String> conceptProps = (List<String>) nodeProps.get(concept);
					for (String property : conceptProps) {
						String relation = concept + "%" + property;
						nodePropList.add(relation);
					}
				}
			}
		}
		// get relationships
		if (metamodel.get(Constants.RELATION) != null) {
			List<Map<String, Object>> edgeList = (List<Map<String, Object>>) metamodel.get(Constants.RELATION);
			for (Map relMap : edgeList) {
				String subject = (String) relMap.get(Constants.FROM_TABLE);
				String object = (String) relMap.get(Constants.TO_TABLE);
				String predicate = (String) relMap.get(Constants.REL_NAME);
				String relation = subject + "@" + predicate + "@" + object;
				relationList.add(relation);
			}
		}
		// get relationship properties
		if (metamodel.get(Constants.RELATION_PROP) != null) {
			Map<String, Object> relPropMap = (Map<String, Object>) metamodel.get(Constants.RELATION_PROP);
			for (String relName : relPropMap.keySet()) {
				List<String> relProps = (List<String>) relPropMap.get(relName);
				for (String property : relProps) {
					String relation = relName + "%" + property;
					relPropList.add(relation);
				}
			}
		}
	}
	
	///////////////////////////////////////////////////////////////////
	//////////////Methods to insert data///////////////////////////////
	///////////////////////////////////////////////////////////////////
	/**
	 * Create all the triples associated with the relationships specified in the prop file
	 * @throws IOException 
	 */
	private void processRelationships(IEngine engine, OWLER owler, CSVFileHelper helper, List<String> headers,
			SemossDataType[] dataTypes, Map<String, Object> metamodel) {
		//TODO user subjects
		// parse metamodel
		Logger logger = getLogger(CLASS_NAME);
		List<String> nodePropList = new ArrayList<String>();
		List<String> relationList = new ArrayList<String>();
		List<String> relPropList = new ArrayList<String>();
		parseMetamodel(metamodel, nodePropList, relationList, relPropList);
		// skip rows
		int startRow = (int) metamodel.get(Constants.START_ROW);
		//start count at 1 just row 1 is the header
		int count = 1;
		while( count<startRow-1 && helper.getNextRow() != null)// && count<maxRows)
		{
			count++;
		}
		String[] values = null;
		Integer endRow = (Integer) metamodel.get(Constants.END_ROW);
		if(endRow == null) {
			endRow = UploadInputUtility.END_ROW_INT;
		}
		while ((values = helper.getNextRow()) != null && count < endRow) {
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
				if(metamodel.containsKey(sub))
				{
					String userSub = metamodel.get(sub).toString(); 
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
				if(metamodel.containsKey(obj))
				{
					String userObj = metamodel.get(obj).toString(); 
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
							if(metamodel.containsKey(prop))
							{
								String userProp = metamodel.get(prop).toString(); 
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
				if(metamodel.containsKey(sub))
				{
					String userSub = metamodel.get(sub).toString(); 
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
					if(metamodel.containsKey(prop))
					{
						String userProp = metamodel.get(prop).toString(); 
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
		metamodel.put(Constants.END_ROW, count);

		System.out.println("FINAL COUNT " + count);
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
		if(type == SemossDataType.INT) {
			retObj = Utility.getInteger(strVal);
		} else if(type == SemossDataType.DOUBLE) {
			retObj = Utility.getDouble(strVal);
		} else if(type == SemossDataType.DATE) {
			Long dTime = SemossDate.getTimeForDate(strVal);
			if(dTime != null) {
				retObj = new SemossDate(dTime, "yyyy-MM-dd");
			}
		} else if(type == SemossDataType.TIMESTAMP) {
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

}
