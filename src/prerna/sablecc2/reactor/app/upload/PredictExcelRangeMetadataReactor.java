package prerna.sablecc2.reactor.app.upload;

import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.Sheet;

import prerna.poi.main.helper.FileHelperUtil;
import prerna.poi.main.helper.excel.ExcelParsing;
import prerna.poi.main.helper.excel.ExcelRange;
import prerna.poi.main.helper.excel.ExcelSheetPreProcessor;
import prerna.poi.main.helper.excel.ExcelWorkbookFileHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class PredictExcelRangeMetadataReactor extends AbstractReactor {

	public static final String SHEET_RANGE = "sheetRange";
	
	public PredictExcelRangeMetadataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SHEET_NAME.getKey(), SHEET_RANGE};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String filePath = this.keyValue.get(this.keysToGet[0]);
		String sheetName = this.keyValue.get(this.keysToGet[1]);
		String sheetRange = this.keyValue.get(this.keysToGet[2]);
		
		// check if file is valid
		if(!ExcelParsing.isExcelFile(filePath)) {
			NounMetadata error = new NounMetadata("Invalid file. Must be .xlsx, .xlsm or .xls", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException e = new SemossPixelException(error);
			e.setContinueThreadOfExecution(false);
			throw e;
		}

		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(filePath);
		Sheet sheet = helper.getSheet(sheetName);
		
		ExcelSheetPreProcessor sheetProcessor = new ExcelSheetPreProcessor(sheet);
		ExcelRange range = new ExcelRange(sheetRange);
		String[] cleanedHeaders = sheetProcessor.getCleanedRangeHeaders(range);
		String[] origHeaders = sheetProcessor.getCleanedRangeHeaders(range);

		Object[][] prediction = ExcelParsing.predictTypes(sheet, sheetRange);
		
		Map<String, Object> rangeMap = new HashMap<String, Object>();
		rangeMap.put("headers", origHeaders);
		rangeMap.put("cleanHeaders", cleanedHeaders);
		Map[] retMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(cleanedHeaders, prediction);
		rangeMap.put("dataTypes", retMaps[0]);
		rangeMap.put("additionalDataTypes", retMaps[1]);
		
		return new NounMetadata(rangeMap, PixelDataType.MAP);
	}

}
