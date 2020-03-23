package prerna.sablecc2.reactor.export;

import prerna.om.ThreadStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ChromeDriverUtility;

public class ExportImageReactor extends AbstractReactor {

	public ExportImageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.BASE_URL.getKey(), ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey() };
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
		NounMetadata retNoun = null;
		String fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (fileLocation == null) {
			String exportName = AbstractExportTxtReactor.getExportFileName("png");
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			// store it in the insight so the FE can download it
			// only from the given insight
			this.insight.addExportFile(exportName, fileLocation);
			retNoun = new NounMetadata(exportName, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		} else {
			retNoun = new NounMetadata(fileLocation, PixelDataType.CONST_STRING);
		}
		ChromeDriverUtility.captureImage(baseUrl, imageUrl, fileLocation, sessionId);
		return retNoun;
	}
}
