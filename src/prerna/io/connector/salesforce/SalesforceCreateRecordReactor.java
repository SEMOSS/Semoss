package prerna.io.connector.salesforce;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SalesforceCreateRecordReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(SalesforceCreateRecordReactor.class);
	
	public SalesforceCreateRecordReactor() {
		this.keysToGet = new String[] { "sObjectName", ReactorKeysEnum.MAP.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String sObjectName = this.keyValue.get(this.keysToGet[0]);
		
		try {
			User user = this.insight.getUser();
			String accessToken = SalesforceUtils.getSalesforceAccessToken(user);
			String instanceUrl = SalesforceUtils.getSalesforceInstanceUrl(user);

			Map<String, Object> fieldValues = getInputFieldMap();
	        if (fieldValues == null || fieldValues.isEmpty()) {
	        	classLogger.error("Input MAP (field-values) missing or empty.");
    			throw new IllegalArgumentException("Input MAP (field-values) missing or empty.");
	        }
			return SalesforceHelper.createRecord(accessToken, instanceUrl, sObjectName, fieldValues);

		} catch (Exception e) {
			classLogger.error("Error creating Salesforce record ", e);
			throw new SemossPixelException("Error creating Salesforce record: " + e.getMessage(), e);
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
		return "Creates a record for a Salesforce object using a user-supplied map of fields and values.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.MAP.getKey())) {
			return "A map of input fields and their values " + ReactorKeysEnum.MAP.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
