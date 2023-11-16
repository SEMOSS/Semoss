package prerna.reactor.export;

import java.io.File;
import java.util.List;
import java.util.UUID;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ToCsvReactor extends AbstractExportTxtReactor {

	private static final String CLASS_NAME = ToCsvReactor.class.getName();
	private static final String APPEND_TIMESTAMP = "appendTimestamp";

	private boolean appendTimestamp = true;

	public ToCsvReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), APPEND_TIMESTAMP};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// throw error is user doesn't have rights to export data
		if(AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}
		this.logger = getLogger(CLASS_NAME);
		this.task = getTask();
		
		this.appendTimestamp = appendTimeStamp();
		
		// set to comma separated
		this.setDelimiter(",");
		
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		
		// get a random file name
		String prefixName =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = getExportFileName(user, prefixName, "csv", appendTimestamp);

		// grab file path to write the file
		this.fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (this.fileLocation == null) {
			String insightFolder = this.insight.getInsightFolder();
			File f = new File(insightFolder);
			if(!f.exists()) {
				f.mkdirs();
			}
			this.fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(true);
		} else {
			this.fileLocation += DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
		}
		
		insightFile.setFilePath(this.fileLocation);
		buildTask();

		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the csv file"));
		return retNoun;
	}
	

	
	private boolean appendTimeStamp() {
		GenRowStruct boolGrs = this.store.getNoun(this.keysToGet[3]);
		if(boolGrs != null) {
			if(boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		
		List<NounMetadata> booleanInput = this.curRow.getNounsOfType(PixelDataType.BOOLEAN);
		if(booleanInput != null && !booleanInput.isEmpty()) {
			return (boolean) booleanInput.get(0).getValue();
		}
		
		return true;
	}

}
