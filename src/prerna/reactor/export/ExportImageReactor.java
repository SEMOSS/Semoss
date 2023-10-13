package prerna.reactor.export;

import java.util.UUID;

import prerna.om.InsightFile;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ExportImageReactor extends AbstractReactor {

	public ExportImageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.BASE_URL.getKey(), ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.IMAGE_WAIT_TIME.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String insightFolder = this.insight.getInsightFolder();
		String baseUrl = this.keyValue.get(this.keysToGet[0]);
		String sessionId = ThreadStore.getSessionId();
		String imageUrl = this.keyValue.get(this.keysToGet[1]);

		// get a random file name
		// grab file path to write the file
		String fileLocation =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey()));
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (fileLocation == null) {
			// get a random file name
			String prefixName =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
			String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "png");
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
		}
	
		Integer waitTime = null;
		String waitTimeStr = this.keyValue.get(this.keysToGet[4]);
		if(waitTimeStr != null && (waitTimeStr=waitTimeStr.trim()).isEmpty()) {
			try {
				waitTime = Integer.parseInt(waitTimeStr);
			} catch(NumberFormatException e) {
				throw new IllegalArgumentException("Invalid wait time option = '" + waitTimeStr + "'. Error is: " + e.getMessage());
			}
		}
		
		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFilePath(fileLocation);
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFileKey(downloadKey);
		this.insight.addExportFile(downloadKey, insightFile);
		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);

		this.insight.getChromeDriver().captureImage(baseUrl, imageUrl, fileLocation, sessionId, waitTime);
		return retNoun;
	}
}
