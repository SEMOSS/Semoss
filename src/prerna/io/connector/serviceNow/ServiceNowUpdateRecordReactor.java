package prerna.io.connector.serviceNow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class ServiceNowUpdateRecordReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowUpdateRecordReactor.class);
	
	private static final String SYS_ID = "sysId";
	private static final String INSTANCE_URL = "instanceURL";

	public ServiceNowUpdateRecordReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TABLE.getKey(), SYS_ID, INSTANCE_URL, ReactorKeysEnum.MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}
	
	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			
			String table = this.keyValue.get(this.keysToGet[0]);
			// ensuring custom table has u_ prefix
			if (table != null && !table.startsWith("u_")) {
				table = "u_" + table;
			}
			
			String sysId = this.keyValue.get(this.keysToGet[1]);
			String instanceURL = this.keyValue.get(this.keysToGet[2]);
			
			User user = this.insight.getUser();
			String accessToken = ServiceNowUtility.getServiceNowAccessToken(user);
			
			Map<String, Object> fieldValues = getInputFieldMap();
			// ensuring custom fields have u_ prefix
			Map<String, Object> updatedFieldValues = new HashMap<>();
			for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
				String fieldName = entry.getKey();
			    Object value = entry.getValue();
			    
			    if (fieldName != null && !fieldName.startsWith("u_")) {
			    	fieldName = "u_" + fieldName;
			    }
			    updatedFieldValues.put(fieldName, value);
			}
			fieldValues = updatedFieldValues;
			
			Map<String, Object> updateRecord = ServiceNowUtility.updateRecord(instanceURL, accessToken, table, sysId, fieldValues);
			
			return new NounMetadata(updateRecord, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			
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
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to update (PATCH) a record in a ServiceNow table via OAuth authentication.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "The name of the ServiceNow table where the record will be updated.";
		} else if (key.equals(SYS_ID)) {
			return "The sys_id of the record to update in the ServiceNow table.";
		} else if (key.equals(INSTANCE_URL)) {
			return "The URL of the ServiceNow user's instance.";
		} else if (key.equals(ReactorKeysEnum.MAP.getKey())) {
			return "The JSON array of field-value maps representing the fields to update in the record.";
		}
		return super.getDescriptionForKey(key);
	}
}