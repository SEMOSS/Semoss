package prerna.sablecc2.reactor.export;

import java.io.File;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ToTsvReactor extends AbstractExportTxtReactor {

	private static final String CLASS_NAME = ToTsvReactor.class.getName();
	
	public ToTsvReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		this.logger = getLogger(CLASS_NAME);
		this.task = getTask();
		// set to tab separated
		this.setDelimiter("\t");
		NounMetadata retNoun = null;
		// get a random file name
		String exportName = getExportFileName("tsv");
		// grab file path to write the file
		this.fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (this.fileLocation == null) {
			String insightFolder = this.insight.getInsightFolder();
			{
				File f = new File(insightFolder);
				if(!f.exists()) {
					f.mkdirs();
				}
			}
			this.fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			// store it in the insight so the FE can download it
			// only from the given insight
			this.insight.addExportFile(exportName, this.fileLocation);
			retNoun = new NounMetadata(exportName, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		}
		buildTask();
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the tsv file"));
		return retNoun;
	}
	
}
