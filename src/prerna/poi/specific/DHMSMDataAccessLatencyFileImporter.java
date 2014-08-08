/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.poi.specific;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.error.FileReaderException;
import prerna.poi.main.AbstractFileReader;

/**
 * Loading data into SEMOSS using Microsoft Excel Loading Sheet files
 */
public class DHMSMDataAccessLatencyFileImporter extends AbstractFileReader {


	private Hashtable<String,String> dataAccessTypeHash = new Hashtable<String,String>();
	private Hashtable<String,String> dataLatencyTypeHash = new Hashtable<String,String>();
	
	/**
	 * Load the excel workbook, determine which sheets to load in workbook from the Loader tab
	 * @param fileName		String containing the absolute path to the excel workbook to load
	 * @throws FileReaderException 
	 */
	public void importFile(String fileName) throws FileReaderException {
		FileInputStream poiReader = null;
		XSSFWorkbook workbook = null;
		try {
			poiReader = new FileInputStream(fileName.replace(";",""));
			workbook = new XSSFWorkbook(poiReader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not find Microsoft Excel File " + fileName.replace(";",""));
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not read Microsoft Excel File " + fileName.replace(";",""));
		} finally {
			try{
				if(poiReader!=null)
					poiReader.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		XSSFSheet sheet = workbook.getSheet("Data Requirements");

		// determine number of sheets to load
		int lastRow = sheet.getLastRowNum();
		for (int rIndex = 2; rIndex <= lastRow; rIndex++) 
		{
			XSSFRow row = sheet.getRow(rIndex);
			// check to make sure cell is not null
			XSSFCell cell = row.getCell(0);
			if(cell != null)
			{
				String dataObject = row.getCell(0).getStringCellValue().replaceAll(" ","_");
				String integrated = row.getCell(3).getStringCellValue();
				String hybrid = row.getCell(2).getStringCellValue();
				String manual = row.getCell(1).getStringCellValue();
				String real = row.getCell(4).getStringCellValue();
				String near = row.getCell(5).getStringCellValue();
				String archive = row.getCell(6).getStringCellValue();

				if (!integrated.isEmpty()) 
					dataAccessTypeHash.put(dataObject,"Integrated");
				else if (!hybrid.isEmpty()) 
					dataAccessTypeHash.put(dataObject,"Hybrid");
				else if (!manual.isEmpty()) 
					dataAccessTypeHash.put(dataObject,"Manual");
				if (!real.isEmpty()) 
					dataLatencyTypeHash.put(dataObject,"Real");
				else if (!near.isEmpty()) 
					dataLatencyTypeHash.put(dataObject,"NearReal");
				else if (!archive.isEmpty()) 
					dataLatencyTypeHash.put(dataObject,"Archive");
				else
					dataLatencyTypeHash.put(dataObject,"Ignore");
			}
		}
	}

	public Hashtable<String,String> getDataAccessTypeHash()
	{
		return dataAccessTypeHash;
	}

	public Hashtable<String,String> getDataLatencyTypeHash()
	{
		return dataLatencyTypeHash;
	}	
	

}
