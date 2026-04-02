package prerna.io.connector.servicenow;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class ServiceNowListTablesReactor extends AbstractReactor {
	
private static final Logger classLogger = LogManager.getLogger(ServiceNowListTablesReactor.class);
	
	private static final String INSTANCE_URL = "instanceURL";
	
	public ServiceNowListTablesReactor() {
		this.keysToGet = new String[] { INSTANCE_URL };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			
			String instanceURL = this.keyValue.get(this.keysToGet[0]);
			
			User user = this.insight.getUser();
			String accessToken = ServiceNowUtility.getServiceNowAccessToken(user);
			
			Map<String, Object> allTables = ServiceNowUtility.getAllTables(instanceURL, accessToken);
			return new NounMetadata(allTables, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Fetches all the tables present in a ServiceNow instance via OAuth authentication.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(INSTANCE_URL)) {
			return "Required URL of the ServiceNow user's instance.";
		}
		return super.getDescriptionForKey(key);
	}

}