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
import prerna.sablecc2.om.ReactorKeysEnum;
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

		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(filePath);
		Sheet sheet = helper.getSheet(sheetName);
		
		ExcelSheetPreProcessor sheetProcessor = new ExcelSheetPreProcessor(sheet);
		ExcelRange range = new ExcelRange(sheetRange);
		String[] cleanedHeaders = sheetProcessor.getCleanedRangeHeaders(range);
		String[] origHeaders = sheetProcessor.getCleanedRangeHeaders(range);

		Object[][] prediction = ExcelParsing.predictTypes(sheet, sheetRange);

		Map[] retMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(cleanedHeaders, prediction);
		Map<String, Map> typeMap = new HashMap<String, Map>();
		typeMap.put("dataTypes", retMaps[0]);
		typeMap.put("additionalDataTypes", retMaps[1]);
		
		Map<String, Object> rangeMap = new HashMap<String, Object>();
		rangeMap.put("headers", origHeaders);
		rangeMap.put("cleanHeaders", cleanedHeaders);
		rangeMap.put("types", typeMap);
		
		return new NounMetadata(rangeMap, PixelDataType.MAP);
	}

}
