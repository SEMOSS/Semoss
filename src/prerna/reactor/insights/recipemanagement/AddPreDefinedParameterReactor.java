package prerna.reactor.insights.recipemanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jasypt.encryption.pbe.StandardPBEByteEncryptor;
import org.jasypt.exceptions.EncryptionOperationNotPossibleException;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.query.parsers.ParamStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AddPreDefinedParameterReactor extends AbstractInsightParameterReactor {

	private static final Logger classLogger = LogManager.getLogger(AddPreDefinedParameterReactor.class);
	
	private static String password = null;

	public AddPreDefinedParameterReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PREDEFINED_PARAM_STRUCT.getKey(), ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		List<Map<String, Object>> paramList = new ArrayList<Map<String, Object>>();
		List<Object> exportVariables = getDetails(this.keysToGet[0]);
		if (exportVariables != null) {
			resolvePayloadVariables(paramList, exportVariables);
		} else  {
			Map<String, Object> paramMap = getParamMap();
			paramList.add(paramMap);
		}
		
		VarStore varStore = this.insight.getVarStore();
		List<String> preDefinedKeys = varStore.getPreDefinedParametersKeys();
		// TODO: not sure if this is accurate?
		// remove all the previous predefined keys to store new updated ones for the current frame
		if(exportVariables != null && !preDefinedKeys.isEmpty()) {
			this.insight.getVarStore().removeAll(preDefinedKeys);
		}
		
		for (Map<String, Object> pMap : paramList) {
			// turn this into a param struct object
			ParamStruct paramStruct = ParamStruct.generateParamStruct(pMap);
			String paramName = paramStruct.getParamName();
			// parameter name must be defined
			if (paramName == null || paramName.isEmpty()) {
				throw new IllegalArgumentException("Parameter name is not defined");
			}
			String variableName = VarStore.PARAM_STRUCT_PD_PREFIX + paramName;
			NounMetadata pStructNoun = new NounMetadata(paramStruct, PixelDataType.PREAPPLIED_PARAM_STRUCT);
			this.insight.getVarStore().put(variableName, pStructNoun);
		}

		NounMetadata retMap = new NounMetadata(paramList, PixelDataType.PREAPPLIED_PARAM_STRUCT);
		return retMap;
	}

	public void resolvePayloadVariables(List<Map<String, Object>> paramList, List<Object> exportVariables) {
		String databaseId = this.keyValue.get(this.keysToGet[1]);
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException(
					"Database " + databaseId + " does not exist or user does not have access to database");
		}

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		for (Object exportVar : exportVariables) {
			Map<String, Object> exportParam = (Map<String, Object>) exportVar;
			if (exportParam.get("type") != null && exportParam.get("list") != null) {
				String queryToTrigger = getDecryptedQuery(exportParam);
				if(queryToTrigger != null) {
					IRawSelectWrapper wrapper = null;
					try {
						// triggering the query to get result set
					    wrapper = WrapperManager.getInstance().getRawWrapper(database, queryToTrigger);
						while (wrapper.hasNext()) {
							Object[] values = wrapper.next().getValues();
							Map<String, Object> paramMap = new HashMap<String, Object>();
							Map<String, Object> paramDetailsMap = new HashMap<String, Object>();
							List<Map<String, Object>> detailsList = new ArrayList<Map<String,Object>>();
							paramMap.put("paramName", values[0]);
							paramMap.put("fillType", "MANUAL");
							paramMap.put("modelDisplay", "freetext");
							paramMap.put("modelLabel", values[0]);
							paramMap.put("defaultValue", values[1]);
							paramMap.put("required", false);
							paramMap.put("appId", databaseId);
							// set flag to distinguish between original param and predefined param
							paramMap.put("isPreApplied", true);
							// can make to values[2] if we include business name in the query
							if(values.length == 3 && values[2] != null) {
								paramDetailsMap.put("columnName", values[2]);
							} else {
								paramDetailsMap.put("columnName", values[0]);
							}
							paramDetailsMap.put("currentValue", values[1]);
							paramDetailsMap.put("appId", databaseId);
							paramDetailsMap.put("operator", "=");
							// below flag is to distinguish between Assisted Query Filters and Preapplied filters
							if (null != (String) exportParam.get("populateInAudit")) {
								String populateInAudit = (String) exportParam.get("populateInAudit");
								if (populateInAudit.equalsIgnoreCase("NO")) {
									paramMap.put("populateInAudit", false);
								} else {
									paramMap.put("populateInAudit", true);
								}
							}
							
							// TODO: this is not valid - will give you false positives 
							// since everything uses the same database id (i.e. Adhoc T)
							PixelList pixelList = this.insight.getPixelList();
							for (Pixel pixel : pixelList) {
								String expression = pixel.getPixelString();
								if (expression.contains(databaseId) && expression.contains("frame")) {
									paramDetailsMap.put("pixelId", pixel.getId());
									paramDetailsMap.put("pixelString", expression);
								}
							}
							detailsList.add(paramDetailsMap);
							paramMap.put("detailsList", detailsList);
							paramList.add(paramMap);
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
				}
			}
		}
	}

	/**
	 * Get the details map
	 * @param keyToPull
	 * @return
	 */
	private List<Object> getDetails(String keyToPull) {
		GenRowStruct grs = this.store.getNoun(keyToPull);
		if (grs != null && !grs.isEmpty()) {
			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if (mapInput != null && !mapInput.isEmpty()) {
				return mapInput;
			}
		}

		return null;
	}
	
	/**
	 * Perform the decryption if necessary
	 * @param exportParam
	 * @return
	 */
	private String getDecryptedQuery(Map<String, Object> exportParam) {
		StandardPBEByteEncryptor encryptor = new StandardPBEByteEncryptor();
		encryptor.setPassword(getPassword());
		byte[] encryptedQuery = getInputQuery(exportParam.get("list").toString());
		if(encryptedQuery != null) {
			try {
				byte[] queryBytes = encryptor.decrypt(encryptedQuery);
				return new String(queryBytes);
			} catch (EncryptionOperationNotPossibleException e) {
				// ignore
				// if the query is not encrypted
				return exportParam.get("list").toString();
			}
		}
		
		return null;
	}
	
	/**
	 * We resolve the URIcomponent
	 * Byte [] 
	 * @return
	 */
	private byte[] getInputQuery(String query) {
		if(query != null && !query.isEmpty()) {
			String stringValue = Utility.decodeURIComponent(query + "");
			return stringValue.getBytes();
		}
		return null;
	}
	
	/**
	 * Get the password from the SMSS file
	 * @return
	 */
	private static String getPassword() {
		if(AddPreDefinedParameterReactor.password != null) {
//			logger.debug("Decrypting with password >> " + AddPreDefinedParameterReactor.password);
			return AddPreDefinedParameterReactor.password;
		}
		
		synchronized (AddPreDefinedParameterReactor.class) {
			if(AddPreDefinedParameterReactor.password != null) {
				return AddPreDefinedParameterReactor.password;
			}
			
			AddPreDefinedParameterReactor.password = DIHelper.getInstance().getProperty(Constants.PM_SEMOSS_EXECUTE_SQL_ENCRYPTION_PASSWORD);
		}
		
//		logger.debug("Decrypting with password >> " + AddPreDefinedParameterReactor.password);
		return AddPreDefinedParameterReactor.password;
	}

}
