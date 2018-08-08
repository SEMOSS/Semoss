package prerna.sablecc2.reactor.insights.save;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.reactor.job.JobReactor;
import prerna.sablecc2.reactor.utils.ImageCaptureReactor;

public class UpdateInsightImageReactor extends AbstractInsightReactor {

	@Override
	public NounMetadata execute() {
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
