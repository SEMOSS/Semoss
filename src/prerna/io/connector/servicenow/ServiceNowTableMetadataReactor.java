package prerna.io.connector.servicenow;

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

public class ServiceNowTableMetadataReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowTableMetadataReactor.class);
	
	private static final String INSTANCE_URL = "instanceURL";
	
	public ServiceNowTableMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TABLE.getKey(), INSTANCE_URL };
		this.keyRequired = new int[] { 1, 1 };
	}
	
	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			
			String table = this.keyValue.get(this.keysToGet[0]);
			String instanceURL = this.keyValue.get(this.keysToGet[1]);
			
			User user = this.insight.getUser();
			String accessToken = ServiceNowUtility.getServiceNowAccessToken(user);
			
			Map<String, Object> tableMetadata = ServiceNowUtility.getTableMetadata(instanceURL, accessToken, table);
			return new NounMetadata(tableMetadata, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Fetches the table metadata like fields, scheme details, etc of the specified ServiceNow table via OAuth authentication.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "Required ServiceNow table name whose metadata we want to retrieve.";
		}else if (key.equals(INSTANCE_URL)) {
			return "Required URL of the ServiceNow user's instance.";
		}
		return super.getDescriptionForKey(key);
	}
}