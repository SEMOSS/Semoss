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
			// ensuring custom table has u_ prefix
			if(table != null && !table.startsWith("u_")) {
				table = "u_" + table.toLowerCase();
			}
			
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
		return "This reactor is used to fetch the table metadata like fields, scheme details, etc of the specified ServiceNow table via OAuth authentication. This reactor is executed after ServiceNow Login.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "The name of the ServiceNow table whose metadata we want to retrieve.";
		}else if (key.equals(INSTANCE_URL)) {
			return "The URL of the ServiceNow user's instance.";
		}
		return super.getDescriptionForKey(key);
	}
}