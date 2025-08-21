package prerna.io.connector.salesforce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SalesforceSoqlQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SalesforceSoqlQueryReactor.class);
	
	private static final String SOQL_QUERY = "soqlQuery";
	
	public SalesforceSoqlQueryReactor() {
		this.keysToGet = new String[] { SOQL_QUERY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String soqlQuery = this.keyValue.get(this.keysToGet[0]);

		try {
			User user = this.insight.getUser();
			String accessToken = SalesforceUtils.getSalesforceAccessToken(user);
			String instanceUrl = SalesforceUtils.getSalesforceInstanceUrl(user);

			return SalesforceHelper.runSoqlQuery(accessToken, instanceUrl, soqlQuery);

		} catch (Exception e) {
			classLogger.error("Error running SOQL query on Salesforce ", e);
			throw new SemossPixelException("Unable to execute SOQL query: " + e.getMessage(), e);
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Executes an arbitrary SOQL query on Salesforce via the REST API and returns the result records.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SOQL_QUERY)) {
			return "This field returns the SOQL query to be executed " + SOQL_QUERY;
		}
		return super.getDescriptionForKey(key);
	}

}
