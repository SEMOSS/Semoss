package prerna.io.connector.serviceNow;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

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

/**
 * Reactor for modifying (PATCH) a record in ServiceNow using OAuth
 * authentication.
 */
public class ServiceNowModifyReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowModifyReactor.class);

	private static SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();

	public ServiceNowModifyReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TABLE.getKey(), ReactorKeysEnum.SYS_ID.getKey(),
				ReactorKeysEnum.TABLE_DATA.getKey(), ReactorKeysEnum.INSTANCE_URL.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	/**
	 * Executes the modification of a record in ServiceNow.
	 *
	 * @return NounMetadata containing the result.
	 */
	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String table = this.keyValue.get(this.keysToGet[0]);
			String sysId = this.keyValue.get(this.keysToGet[1]);
			String tableData = this.keyValue.get(this.keysToGet[2]);
			String instanceURL = this.keyValue.get(this.keysToGet[3]);
			String accessToken = getAccessToken();
			ObjectMapper mapper = new ObjectMapper();
			Map<String, String> data = mapper.readValue(tableData, new TypeReference<Map<String, String>>() {
			});
			Map<String, Object> modifyRecord = ServiceNowUtility.modifyRecord(instanceURL, accessToken, table, sysId,
					data);
			return new NounMetadata(modifyRecord, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Retrieves the OAuth access token for ServiceNow.
	 *
	 * @return Access token as String.
	 */
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

	/**
	 * Returns a description of this reactor.
	 *
	 * @return Description string.
	 */
	@Override
	public String getReactorDescription() {
		return "This reactor is used to modify (PATCH) a record in a ServiceNow table via OAuth authentication.";
	}

	/**
	 * Returns a description for a specific key.
	 *
	 * @param key The key to describe.
	 * @return Description string.
	 */
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "The name of the ServiceNow table where the record will be modified.";
		}
		if (key.equals(ReactorKeysEnum.SYS_ID.getKey())) {
			return "The sys_id of the record to modify in the ServiceNow table.";
		}
		if (key.equals(ReactorKeysEnum.TABLE_DATA.getKey())) {
			return "The JSON array of field-value maps representing the fields to update in the record.";
		}
		return super.getDescriptionForKey(key);
	}
}
