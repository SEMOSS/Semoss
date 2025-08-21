package prerna.io.connector.salesforce;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SalesforceObjectSchemaReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SalesforceObjectSchemaReactor.class);
	
	private static final String SOBJECT_NAME = "sObjectName";
	
	public SalesforceObjectSchemaReactor() {
		this.keysToGet = new String[] { SOBJECT_NAME };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String sObjectName = this.keyValue.get(this.keysToGet[0]);

		try {
			User user = this.insight.getUser();
			String accessToken = SalesforceUtils.getSalesforceAccessToken(user);
			String instanceUrl = SalesforceUtils.getSalesforceInstanceUrl(user);

			return SalesforceHelper.fetchObjectSchema(accessToken, instanceUrl, sObjectName);

		} catch (Exception e) {
			classLogger.error("Error while fetching Salesforce metadata ", e);
			throw new SemossPixelException("Unable to retrieve Salesforce metadata: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Fetches all Salesforce sObjects and (optionally) fields metadata for a specific object using the REST API.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SOBJECT_NAME)) {
			return "This field specifies the name of the Salesforce object " + SOBJECT_NAME;
		}
		return super.getDescriptionForKey(key);
	}

}
