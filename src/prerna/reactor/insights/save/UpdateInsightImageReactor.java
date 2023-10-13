package prerna.reactor.insights.save;

import org.apache.logging.log4j.Logger;

import prerna.om.ThreadStore;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.reactor.utils.ImageCaptureReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateInsightImageReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = UpdateInsightImageReactor.class.getName();
	
	public UpdateInsightImageReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), 
				ReactorKeysEnum.URL.getKey(), ReactorKeysEnum.IMAGE_WAIT_TIME.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Starting image capture...");
		logger.info("Operation can take up to 10 seconds to complete");
		
		String projectId = getProject();
		String rdbmsId = getRdbmsId();
		String feUrl = getUrl();
		String sessionId = ThreadStore.getSessionId();
		Object params = getExecutionParams();
		
		Integer waitTime = null;
		String waitTimeStr = this.keyValue.get(this.keysToGet[3]);
		if(waitTimeStr != null && (waitTimeStr=waitTimeStr.trim()).isEmpty()) {
			try {
				waitTime = Integer.parseInt(waitTimeStr);
			} catch(NumberFormatException e) {
				throw new IllegalArgumentException("Invalid wait time option = '" + waitTimeStr + "'. Error is: " + e.getMessage());
			}
		}
		
		if(params == null) {
			ImageCaptureReactor.runImageCapture(feUrl, projectId, rdbmsId, null, sessionId, waitTime);
		} else {
			ImageCaptureReactor.runImageCapture(feUrl, projectId, rdbmsId, params.toString(), sessionId, waitTime);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
