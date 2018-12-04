package prerna.poi.main.helper.excel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.poi.ss.usermodel.Sheet;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

/**
 * This class stores and retrieves the data validation map from the database
 *
 */
public class ExcelDataValidationReactor extends AbstractReactor {

	public ExcelDataValidationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.SHEET_NAME.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String filePath = this.keyValue.get(this.keysToGet[0]);
		if(!ExcelParsing.isExcelFile(filePath)) {
			NounMetadata error = new NounMetadata("Invalid file. Must be .xlsx, .xlsm or .xls", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException e = new SemossPixelException(error);
			e.setContinueThreadOfExecution(false);
			throw e;
		}
		String appName = this.keyValue.get(this.keysToGet[1]);
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(filePath);
		List<String> sheetNames = new Vector<>();
		String sheetName = this.keyValue.get(this.keysToGet[2]);
		// TODO get modified headers
		Map<String, String> newHeaders = new HashMap<>();
		if (sheetName == null) {
			sheetNames = helper.getSheets();
		} else {
			sheetNames.add(sheetName);
		}
		Map<String, Object> retMap = new HashMap<>();
		for (String sheet : sheetNames) {
			Sheet excelSheet = helper.getSheet(sheet);
			Map<String, Object> dataValidationMap = ExcelDataValidationHelper.getDataValidation(excelSheet, newHeaders);
			Map<String, Object> form = ExcelDataValidationHelper.createInsertForm(appName, sheet, dataValidationMap, null);
			if (!form.isEmpty()) {
				retMap.put(sheet, form);
			}
		}

		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}
