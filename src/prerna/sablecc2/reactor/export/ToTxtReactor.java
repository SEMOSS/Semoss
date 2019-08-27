package prerna.sablecc2.reactor.export;

import java.util.List;
import java.util.UUID;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class ToTxtReactor extends AbstractExportTxtReactor {

	private static final String CLASS_NAME = ToTxtReactor.class.getName();
	
	public ToTxtReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.DELIMITER.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		this.logger = getLogger(CLASS_NAME);
		this.task = getTask();
		// get the input delimiter
		this.setDelimiter(getDelimiter());
		NounMetadata retNoun = null;
		// get a random file name
		String randomKey = UUID.randomUUID().toString();
		// grab file path to write the file
		this.fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (this.fileLocation == null) {
			this.fileLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + randomKey + ".txt";
			// store it in the insight so the FE can download it
			// only from the given insight
			this.insight.addExportFile(randomKey, this.fileLocation);
			retNoun = new NounMetadata(randomKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		} else {
			retNoun = NounMetadata.getSuccessNounMessage("Successfully generated the txt file.");
		}
		buildTask();
		return retNoun;
	}
	
	private String getDelimiter() {
		GenRowStruct grs = store.getNoun(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		
		List<String> values = this.curRow.getAllStrValues();
		if(values != null && !values.isEmpty()) {
			return values.get(0);
		}
		
		throw new IllegalArgumentException("Must define the delimiter");
	}

}
