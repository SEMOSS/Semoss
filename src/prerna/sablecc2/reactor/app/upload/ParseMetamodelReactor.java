package prerna.sablecc2.reactor.app.upload;

import java.util.Map;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ParseMetamodelReactor extends AbstractReactor {
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public ParseMetamodelReactor() {
		this.keysToGet = new String[] { UploadInputUtility.FILE_PATH, UploadInputUtility.DELIMITER,
				UploadInputUtility.ROW_COUNT, UploadInputUtility.PROP_FILE };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String csvFilePath = UploadInputUtility.getFilePath(this.store);
		String delimiter = UploadInputUtility.getDelimiter(this.store);
		char delim = delimiter.charAt(0);
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delim);
		helper.parse(csvFilePath);
		return new NounMetadata(generateMetaModelFromProp(helper), PixelDataType.MAP);
	}

	/**
	 * Generates the Meta model data based on the definition of the prop file
	 */
	private Map<String, Object> generateMetaModelFromProp(CSVFileHelper helper) {
		Map<String, Object> metamodel = UploadInputUtility.getMetamodelFromPropFile(this.store);
		if (metamodel == null) {
			String error = "Unable to read metamodel prop file.";
			NounMetadata noun = new NounMetadata(error, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// get file location and file name
		String filePath = helper.getFileLocation();
		String file = filePath.substring(filePath.lastIndexOf(DIR_SEPARATOR) + DIR_SEPARATOR.length(),
				filePath.lastIndexOf("."));
		try {
			file = file.substring(0, file.indexOf("_____UNIQUE"));
		} catch (Exception e) {
			// just in case that fails, this shouldnt because if its a filename
			// it should have a "."
			file = filePath.substring(filePath.lastIndexOf(DIR_SEPARATOR) + DIR_SEPARATOR.length(),
					filePath.lastIndexOf("."));
		}
		
		// store file path and file name to send to FE
		metamodel.put("fileLocation", filePath);
		metamodel.put("fileName", file);

		// fileMetaModelData.put("additionalDataTypes",
		// predictor.getAdditionalDataTypeMap());
		// store auto modified header names
		metamodel.put("headerModifications", helper.getChangedHeaders());
		// need to close the helper
		helper.clear();

		return metamodel;
	}
}
