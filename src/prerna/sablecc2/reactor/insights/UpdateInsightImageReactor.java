package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.utils.ImageCaptureReactor;

public class UpdateInsightImageReactor extends AbstractInsightReactor {

	@Override
	public NounMetadata execute() {
		String engineName = getApp();
		String rdbmsId = getRdbmsId();
		String feUrl = getUrl();
		ImageCaptureReactor.runImageCapture(feUrl, engineName, rdbmsId);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
