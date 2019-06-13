package prerna.sablecc2.reactor.insights.save;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.reactor.job.JobReactor;
import prerna.sablecc2.reactor.utils.ImageCaptureReactor;

public class UpdateInsightImageReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = UpdateInsightImageReactor.class.getName();
	
	public UpdateInsightImageReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.URL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Starting image capture...");
		logger.info("Operation can take up to 10 seconds to complete");
		
		String appId = getApp();
		String rdbmsId = getRdbmsId();
		String feUrl = getUrl();
		String sessionId = this.planner.getVariable(JobReactor.SESSION_KEY).getValue().toString();
		Object params = getExecutionParams();
		if(params == null) {
			ImageCaptureReactor.runImageCapture(feUrl, appId, rdbmsId, null, sessionId);
		} else {
			ImageCaptureReactor.runImageCapture(feUrl, appId, rdbmsId, params.toString(), sessionId);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
