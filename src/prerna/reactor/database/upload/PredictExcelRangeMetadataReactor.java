/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.database.upload;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.Sheet;

import prerna.poi.main.helper.FileHelperUtil;
import prerna.poi.main.helper.excel.ExcelParsing;
import prerna.poi.main.helper.excel.ExcelRange;
import prerna.poi.main.helper.excel.ExcelSheetPreProcessor;
import prerna.poi.main.helper.excel.ExcelWorkbookFileHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class PredictExcelRangeMetadataReactor extends AbstractReactor {

	public static final String SHEET_RANGE = "sheetRange";

	public PredictExcelRangeMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.SHEET_NAME.getKey(), SHEET_RANGE, ReactorKeysEnum.PASSWORD.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		if (!new File(filePath).exists()) {
			throw new IllegalArgumentException("Unable to locate file");
		}
		String sheetName = this.keyValue.get(this.keysToGet[2]);
		String sheetRange = this.keyValue.get(this.keysToGet[3]);
		String password = this.keyValue.get(this.keysToGet[4]);

		// check if file is valid
		if (!ExcelParsing.isExcelFile(filePath)) {
			NounMetadata error = new NounMetadata("Invalid file. Must be .xlsx, .xlsm or .xls",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException e = new SemossPixelException(error);
			e.setContinueThreadOfExecution(false);
			throw e;
		}

		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(filePath, password);
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
