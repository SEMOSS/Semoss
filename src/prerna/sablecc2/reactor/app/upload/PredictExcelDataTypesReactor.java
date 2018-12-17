package prerna.sablecc2.reactor.app.upload;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.poi.main.helper.FileHelperUtil;
import prerna.poi.main.helper.excel.ExcelBlock;
import prerna.poi.main.helper.excel.ExcelParsing;
import prerna.poi.main.helper.excel.ExcelRange;
import prerna.poi.main.helper.excel.ExcelSheetPreProcessor;
import prerna.poi.main.helper.excel.ExcelWorkbookFilePreProcessor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class PredictExcelDataTypesReactor extends AbstractReactor {
	private static final String CLASS_NAME = PredictExcelDataTypesReactor.class.getName();

	public PredictExcelDataTypesReactor() {
		this.keysToGet = new String[] { UploadInputUtility.FILE_PATH };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		int stepCounter = 1;
		String filePath = this.keyValue.get(UploadInputUtility.FILE_PATH);
		// check if file is valid
		if(!ExcelParsing.isExcelFile(filePath)) {
			NounMetadata error = new NounMetadata("Invalid file. Must be .xlsx, .xlsm or .xls", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException e = new SemossPixelException(error);
			e.setContinueThreadOfExecution(false);
			throw e;
		}
		Map<String, Object> fileData = new HashMap<String, Object>();

//		// fileLocation
//		fileData.put("fileLocation", filePath);
//		String file = filePath.substring(filePath.lastIndexOf("\\") + 1, filePath.lastIndexOf("."));
//		try {
//			// taking out the date added onto the original file name
//			file = file.substring(0, file.indexOf("_____UNIQUE"));
//		} catch (Exception e) {
//			// just in case that fails, this shouldnt because if its a filename
//			// it should have a "."
//			file = filePath.substring(filePath.lastIndexOf("\\") + 1, filePath.lastIndexOf("."));
//		}
//		fileData.put("fileName", file);

		// processing excel file data
		// trying to determine data blocks within a sheet
		ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();
		logger.info(stepCounter+ ". Parsing file");
		preProcessor.parse(filePath);
		logger.info(stepCounter+". Done");
		stepCounter++;
		
		logger.info(stepCounter+ ". Determining sheet range");
		preProcessor.determineTableRanges();
		logger.info(stepCounter+ ". Done");
		stepCounter++;
		
		logger.info(stepCounter + ". Processing all sheets");

		Map<String, ExcelSheetPreProcessor> sProcessors = preProcessor.getSheetProcessors();
		for (String sheet : sProcessors.keySet()) {
			logger.info("Processing sheet: " + sheet);
			ExcelSheetPreProcessor processor = sProcessors.get(sheet);
			List<ExcelBlock> blocks = processor.getAllBlocks();
			Map<String, Object> rangeInfo = new HashMap<String, Object>();
			for (ExcelBlock block : blocks) {
				List<ExcelRange> ranges = block.getRanges();
				for (ExcelRange r : ranges) {
					String rSyntax = r.getRangeSyntax();
					logger.info("Processing range: " + rSyntax);
					String[] origHeaders = processor.getRangeHeaders(r);
					String[] cleanedHeaders = processor.getCleanedRangeHeaders(r);
					Object[][] rangeTypes = block.getRangeTypes(r);
					Map[] retMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(cleanedHeaders, rangeTypes);
					Map<String, Object> rangeMap = new HashMap<String, Object>();
					logger.info("Obtaining headers and types");
					rangeMap.put("headers", origHeaders);
					rangeMap.put("cleanHeaders", cleanedHeaders);
					rangeMap.put("dataTypes", retMaps[0]);
					rangeMap.put("additionalDataTypes", retMaps[1]);
					rangeInfo.put(rSyntax, rangeMap);
				}
			}

			// add all ranges in the sheet
			fileData.put(sheet, rangeInfo);
			preProcessor.clear();
		}
		logger.info(stepCounter + ". Done");

		// store the info
		return new NounMetadata(fileData, PixelDataType.MAP);
	}
}
