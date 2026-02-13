package prerna.io.connector.serviceNow;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.ServiceNowUtility;

public class ServiceNowRetrieveRecordsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowRetrieveRecordsReactor.class);
	
	private static final String INSTANCE_URL = "instanceURL";
	
	public ServiceNowRetrieveRecordsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TABLE.getKey(), ReactorKeysEnum.LIMIT.getKey(), INSTANCE_URL };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String table = this.keyValue.get(this.keysToGet[0]);
			String limit = this.keyValue.get(this.keysToGet[1]);
			String instanceURL = this.keyValue.get(this.keysToGet[2]);
			
			User user = this.insight.getUser();
			String accessToken = ServiceNowUtility.getServiceNowAccessToken(user);
			
			List<Map<String, Object>> allRecords = ServiceNowUtility.getAllRecords(instanceURL, accessToken, table, limit);
			return new NounMetadata(allRecords, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to retrieve records present in a ServiceNow table via OAuth authentication.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "The name of the ServiceNow table from where the records will be retrieved.";
		}else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The number of records user wants to retrieve from a ServiceNow table.";
		}else if (key.equals(INSTANCE_URL)) {
			return "The URL of the ServiceNow user's instance.";
		}
		return super.getDescriptionForKey(key);
	}
}
