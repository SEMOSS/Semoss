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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.error.FileReaderException;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class DHMSMIntegrationSavingsPerYearFromWkSht {
	private static final Logger LOGGER = LogManager.getLogger(DHMSMIntegrationSavingsPerYearFromWkSht.class.getName());
	
	private XSSFWorkbook wb;
	private String workingDir;
	private String folder;
	private String templateName;
	private ArrayList<String> systems = new ArrayList<String>();
	
	public DHMSMIntegrationSavingsPerYearFromWkSht() {
		workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Reports" + System.getProperty("file.separator");
		templateName = "TAP-High Probability System Analysis.xlsx";
	}
	
	public void read() throws FileReaderException {
		if(wb == null) {
			try {
				wb = (XSSFWorkbook) WorkbookFactory.create(new File(workingDir + folder + templateName));
			} 
			catch (InvalidFormatException e) {
				e.printStackTrace();
				throw new FileReaderException("Could not find template for report.");
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileReaderException("Could not find template for report.");
			} 
		}
		
		XSSFSheet reportSheet = wb.getSheetAt(0);
		for(Row row : reportSheet) {
			if(row.getCell(5).getStringCellValue().equals("Yes")) {
				systems.add(row.getCell(0).getStringCellValue());
			}
		}
	}
	
	public ArrayList<String> getSystems() {
		return systems;
	}

}
