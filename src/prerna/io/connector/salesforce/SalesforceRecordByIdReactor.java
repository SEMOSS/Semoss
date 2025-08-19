package prerna.io.connector.salesforce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SalesforceRecordByIdReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SalesforceRecordByIdReactor.class);
	
	public SalesforceRecordByIdReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SOBJECT_NAME.getKey(), ReactorKeysEnum.RECORD_ID.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String sObjectName = this.keyValue.get(this.keysToGet[0]);
		String recordId = this.keyValue.get(this.keysToGet[1]);

		try {
			User user = this.insight.getUser();
			String accessToken = SalesforceUtils.getSalesforceAccessToken(user);
			String instanceUrl = SalesforceUtils.getSalesforceInstanceUrl(user);

			return SalesforceHelper.fetchRecordById(accessToken, instanceUrl, sObjectName, recordId);

		} catch (Exception e) {
			classLogger.error("Error while fetching Salesforce record by Id ", e);
			throw new SemossPixelException("Unable to retrieve Salesforce record: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Fetches a Salesforce record by sObject name and record Id using the REST API.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.SOBJECT_NAME.getKey())) {
			return "This field specifies the name of the Salesforce object " + ReactorKeysEnum.SOBJECT_NAME.getKey();
		} else if (key.equals(ReactorKeysEnum.RECORD_ID.getKey())) {
			return "The Salesforce record Id to fetch " + ReactorKeysEnum.RECORD_ID.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
