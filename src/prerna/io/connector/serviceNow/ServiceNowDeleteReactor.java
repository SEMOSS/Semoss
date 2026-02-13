package prerna.io.connector.serviceNow;

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

public class ServiceNowDeleteReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowDeleteReactor.class);
	
	private static final String SYS_ID = "sysId";
	private static final String INSTANCE_URL = "instanceURL";

	public ServiceNowDeleteReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TABLE.getKey(), SYS_ID, INSTANCE_URL };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String table = this.keyValue.get(this.keysToGet[0]);
			String sysId = this.keyValue.get(this.keysToGet[1]);
			String instanceURL = this.keyValue.get(this.keysToGet[2]);
			
			User user = this.insight.getUser();
			String accessToken = ServiceNowUtility.getServiceNowAccessToken(user);
			
			Map<String, Object> deleteRecord = ServiceNowUtility.deleteRecord(instanceURL, accessToken, table, sysId);
			return new NounMetadata(deleteRecord, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to delete a record from a ServiceNow table via OAuth authentication.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "The name of the ServiceNow table from which the record will be deleted.";
		} else if (key.equals(SYS_ID)) {
			return "The sys_id of the record to delete in the ServiceNow table.";
		} else if (key.equals(INSTANCE_URL)) {
			return "The URL of the ServiceNow user's instance.";
		}
		return super.getDescriptionForKey(key);
	}
}
