package prerna.io.connector.serviceNow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.ServiceNowUtility;

public class ServiceNowCreateRecordReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowCreateRecordReactor.class);

	private static final String INSTANCE_URL = "instanceURL";
	
	public ServiceNowCreateRecordReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TABLE.getKey(), INSTANCE_URL, ReactorKeysEnum.MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String table = this.keyValue.get(this.keysToGet[0]);
			String instanceURL = this.keyValue.get(this.keysToGet[1]);
			String accessToken = getAccessToken();
			Map<String, Object> fieldValues = getInputFieldMap();
			Map<String, Object> record = ServiceNowUtility.createRecord(instanceURL, accessToken, table, fieldValues);
			return new NounMetadata(record, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> getInputFieldMap() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.MAP.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if(mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, Object>) mapNouns.get(0).getValue();
			}
		}

		List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapNouns != null && !mapNouns.isEmpty()) {
			return (Map<String, Object>) mapNouns.get(0).getValue();
		}
		return null;
	}

	private String getAccessToken() {
		String accessToken = null;
		User user = this.insight.getUser();
		try {
			if (user == null) {
				Map<String, Object> retMap = new HashMap<>();
				retMap.put("type", "servicenow");
				retMap.put("message", "Please login to your ServiceNow account");
				classLogger.error("user can not be null");
				throwLoginError(retMap);
			} else {
				AccessToken msToken = user.getAccessToken(AuthProvider.SERVICENOW);
				accessToken = msToken.getAccess_token();
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", "servicenow");
			retMap.put("message", "Please login to your ServiceNow account");
			classLogger.error("Error while getting access token");
			throwLoginError(retMap);
		}
		return accessToken;
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to create a new record in a ServiceNow table via OAuth authentication.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "The name of the ServiceNow table where the record will be created.";
		} else if (key.equals(INSTANCE_URL)) {
			return "The URL of the ServiceNow user's instance.";
		} else if (key.equals(ReactorKeysEnum.MAP.getKey())) {
			return "The JSON array of field-value maps representing the record(s) to create in the table.";
		}
		return super.getDescriptionForKey(key);
	}
}
