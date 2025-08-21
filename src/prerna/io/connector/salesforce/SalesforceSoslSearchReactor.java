package prerna.io.connector.salesforce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SalesforceSoslSearchReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(SalesforceSoslSearchReactor.class);
	
	private static final String SOSL_QUERY = "soslQuery";
	
	public SalesforceSoslSearchReactor() {
		this.keysToGet = new String[] { SOSL_QUERY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String soslQuery = this.keyValue.get(this.keysToGet[0]);

		try {
			User user = this.insight.getUser();
			String accessToken = SalesforceUtils.getSalesforceAccessToken(user);
			String instanceUrl = SalesforceUtils.getSalesforceInstanceUrl(user);

			return SalesforceHelper.runSoslQuery(accessToken, instanceUrl, soslQuery);

		} catch (Exception e) {
			classLogger.error("Error running SOSL query on Salesforce ", e);
			throw new SemossPixelException("Unable to execute SOSL query: " + e.getMessage(), e);
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Executes a Salesforce SOSL search against the org, returning records that match the query.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SOSL_QUERY)) {
			return "This field returns the SOSL query to be executed " + SOSL_QUERY;
		}
		return super.getDescriptionForKey(key);
	}
	
}
