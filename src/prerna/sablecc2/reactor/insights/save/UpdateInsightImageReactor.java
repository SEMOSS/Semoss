package prerna.sablecc2.reactor.insights.save;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.reactor.utils.ImageCaptureReactor;

public class UpdateInsightImageReactor extends AbstractInsightReactor {

	@Override
	public NounMetadata execute() {
		String engineName = getApp();
		String rdbmsId = getRdbmsId();
		String feUrl = getUrl();
		Object params = getExecutionParams();
		if(params == null) {
			ImageCaptureReactor.runImageCapture(feUrl, engineName, rdbmsId, null);
		} else {
			ImageCaptureReactor.runImageCapture(feUrl, engineName, rdbmsId, params.toString());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
