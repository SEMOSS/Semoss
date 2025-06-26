package prerna.reactor.report;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.Settings;
import prerna.util.Utility;

public class SessionSummaryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SessionSummaryReactor.class);

	private static final String SESSION_SUMMARY = "session-summary";
	private static final String ENGINESTATS = "engine-stats";
	private static final String USAGEASUMMARY = "usage-summary";
	private String loggerMicroserviceUrl = null;

	public SessionSummaryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.REPORT_NAME.getKey() };
		this.loggerMicroserviceUrl = Utility.getDIHelperProperty(Settings.LOGGER_MICROSERVICE_URL);
	}; 

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String userId = this.insight.getUserId();
		if (userId == null || userId.isEmpty()) {
			throw new IllegalArgumentException("User is not properly logged in.");
		}
		String endPoint = null;
		if(this.keyValue.get(ReactorKeysEnum.REPORT_NAME.getKey()).equalsIgnoreCase("stats")) {
			endPoint = ENGINESTATS;
		}
		else if (this.keyValue.get(ReactorKeysEnum.REPORT_NAME.getKey()).equalsIgnoreCase("usage")) {
			endPoint = USAGEASUMMARY;
		}
		else {
			endPoint = SESSION_SUMMARY;
		}

		String url = this.loggerMicroserviceUrl +"/"+ endPoint;
		 String response = HttpHelperUtility.getRequest(url, null, null, null, null);
		return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.LOGGING_DATA);
	}

	protected GenRowFilters getFilters() {
		GenRowStruct inputsGRS = this.store.getNoun(ReactorKeysEnum.FILTERS.getKey());
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			NounMetadata filterNoun = inputsGRS.getNoun(0);
			SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
			GenRowFilters filters = qs.getCombinedFilters();
			return filters;
		}
		return null;
	}

}
