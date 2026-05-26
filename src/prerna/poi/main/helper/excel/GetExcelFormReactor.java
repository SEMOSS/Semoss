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
package prerna.poi.main.helper.excel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Sheet;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * This class stores and retrieves the data validation map from the database
 *
 */
public class GetExcelFormReactor extends AbstractReactor {

	public GetExcelFormReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.DATABASE.getKey(),
				ReactorKeysEnum.SHEET_NAME.getKey() };
	}

	/**
	 * Builds insert-form metadata from Excel data validations for the requested
	 * sheet(s).
	 *
	 * @return map of sheet name to generated form metadata
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String filePath = this.keyValue.get(this.keysToGet[0]);
		if (!ExcelParsing.isExcelFile(filePath)) {
			NounMetadata error = new NounMetadata("Invalid file. Must be .xlsx, .xlsm or .xls",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException e = new SemossPixelException(error);
			e.setContinueThreadOfExecution(false);
			throw e;
		}
		// TODO should pass in databaseId
		String databaseName = this.keyValue.get(this.keysToGet[1]);
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(filePath);
		List<String> sheetNames = new ArrayList<>();
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
			Map<String, Object> form = ExcelDataValidationHelper.createInsertForm(databaseName, sheet,
					dataValidationMap, null);
			if (!form.isEmpty()) {
				retMap.put(sheet, form);
			}
		}

		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}
