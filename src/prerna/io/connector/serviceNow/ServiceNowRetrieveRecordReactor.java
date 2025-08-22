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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.ServiceNowUtility;
import prerna.util.SocialPropertiesUtil;

public class ServiceNowRetrieveRecordReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowRetrieveRecordReactor.class);

	public ServiceNowRetrieveRecordReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TABLE.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.INSTANCE_URL.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	private static SocialPropertiesUtil socialData = null;
	static {
		socialData = SocialPropertiesUtil.getInstance();
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String table = this.keyValue.get(this.keysToGet[0]);
			String limit = this.keyValue.get(this.keysToGet[1]);
			String instanceURL = this.keyValue.get(this.keysToGet[2]);
			String accessToken = getAccessToken();
			String tableName = "u_testtable3";
			List<Map<String, Object>> allRecords = ServiceNowUtility.getAllRecords(instanceURL, accessToken, table,
					limit);
			return new NounMetadata(allRecords, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	private String getAccessToken() {
		String accessToken = null;
		User user = this.insight.getUser();
		try {
			if (user == null) {
				Map<String, Object> retMap = new HashMap<>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				classLogger.error("user can not be null");
				throwLoginError(retMap);
			} else {
				AccessToken msToken = user.getAccessToken(AuthProvider.SERVICENOW);
				accessToken = msToken.getAccess_token();
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			classLogger.error("Error while getting access token");
			throwLoginError(retMap);
		}
		return accessToken;
	}
}
