/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Utility;

public class SysCapSimHeatMapWriter
{
	public void exportExcelPercent(String fileLoc, String templateFileLoc, ArrayList<String> capabilities, ArrayList<String> systems,
			Hashtable<String, Hashtable<String, Hashtable<String, Object>>> paramDataHash)
	{
		XSSFWorkbook wb;
		// if a report template exists, then create a copy of the template, otherwise create a new workbook
		if (templateFileLoc != null)
		{
			try
			{
				wb = (XSSFWorkbook) WorkbookFactory.create(new File(templateFileLoc));
			} catch (InvalidFormatException e)
			{
				wb = new XSSFWorkbook();
			} catch (IOException e)
			{
				wb = new XSSFWorkbook();
			}
		} else
		{
			wb = new XSSFWorkbook();
		}
		
		XSSFSheet sheet = wb.getSheet("Heat Map Percentages");


		Hashtable<String, Hashtable<String, Object>> allData = paramDataHash.get("Data_Objects_Created");
		Hashtable<String, Hashtable<String, Object>> allBLU = paramDataHash.get("Business_Logic_Provided");
		Hashtable<String, Object> systemData = new Hashtable<String, Object>();
		Hashtable<String, Object> systemBLU = new Hashtable<String, Object>();
		ArrayList<String> capabilityNames = capabilities;
		ArrayList<String> systemNames = systems;
		
		java.util.Collections.sort(capabilityNames);
		java.util.Collections.sort(systemNames);

		int column = 1;
		int row = 2;
		
		XSSFRow selectedRow = sheet.getRow(1);
		XSSFCell selectedCell;
		XSSFCellStyle systemStyle = sheet.getRow(2).getCell(0).getCellStyle();
		XSSFCellStyle capabilityStyle = sheet.getRow(1).getCell(1).getCellStyle();
		XSSFCellStyle dataStyle = sheet.getRow(2).getCell(1).getCellStyle();

		for (String capability : capabilityNames)
		{
			selectedCell = selectedRow.createCell(column);
			selectedCell.setCellStyle(capabilityStyle);
			selectedCell.setCellValue(capability);
			column++;
		}
		
		for (String systemKey : systemNames)
		{
			selectedRow = sheet.createRow(row);
			selectedCell = selectedRow.createCell(0);
			selectedCell.setCellValue(systemKey);
			selectedCell.setCellStyle(systemStyle);

			for (int columnMinusOne = 0; columnMinusOne < capabilityNames.size(); columnMinusOne++)
			{
				String key = capabilityNames.get(columnMinusOne) + "-" + systemKey;
				
				selectedCell = selectedRow.createCell(columnMinusOne + 1);
				selectedCell.setCellStyle(dataStyle);
				Object dataScore;
				Object bluScore;
				if (allData.get(key) != null)
					dataScore = allData.get(key).get("Score");
				else
					dataScore = 0.0;
				if (allBLU.get(key) != null)
					bluScore = allBLU.get(key).get("Score");
				else
					bluScore = 0.0;

				selectedCell.setCellValue((((Double) dataScore) + ((Double) bluScore)) / 200); // Because cell format is set
																								// as percentage
			}
			row++;
		}
		
		Utility.writeWorkbook(wb, fileLoc);
	}
}
