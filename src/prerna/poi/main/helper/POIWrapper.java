package prerna.poi.main.helper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;


public class POIWrapper implements IRawSelectWrapper {

	String excelFileName = null;
	String sheetName = null;
	int startCol = 0;
	int endCol = 0;
	int startRow = 0;
	int endRow = 0;
	Sheet sheet = null;
	int count = 0;
	String [] headers = null;
	
	Workbook book = null;
	
	public POIWrapper(String fileName, String sheetName2, int startCol2,
			int startRow2, int endCol2, int endRow2) {
		// TODO Auto-generated constructor stub
		this.excelFileName = fileName;
		this.sheetName = sheetName2;
		this.startCol = startCol2;
		this.startRow = startRow2;
		this.endCol = endCol2;
		this.endRow = endRow2;
	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		if(sheet == null)
		{
			try {
				book = WorkbookFactory.create(new FileInputStream(excelFileName));
				sheet = book.getSheet(sheetName);				
				count = startRow;
				
			} catch (EncryptedDocumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		

	}

	@Override
	public void setQuery(String query) {
		// TODO Auto-generated method stub
		// do nothing for no

	}

	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub
		try {
			if(book != null)
				book.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void setEngine(IEngine engine) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return count < endRow;
	}

	@Override
	public IHeadersDataRow next() {
		// TODO Auto-generated method stub
		Row thisRow = sheet.getRow(count);
		Object [] data = new String[endCol - startCol];
		for(int colIndex = startCol, dCount = 0;colIndex < endCol;colIndex++, dCount++)
			data[dCount] = getCell(thisRow.getCell(colIndex));
		count++;
		return new ExcelHeadersDataRow(headers, data);
	}

	@Override
	public String[] getHeaders() {
		// TODO Auto-generated method stub
		if(headers == null && count < endRow)
		{
			Row thisRow = sheet.getRow(count);
			headers = new String[endCol - startCol];
			for(int colIndex = startCol, hCount = 0;colIndex < endCol;colIndex++, hCount++)
				headers[hCount] = getCell(thisRow.getCell(colIndex));
			count++;
		}
		return headers;
	}

	@Override
	public SemossDataType[] getTypes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getNumRecords() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}
	
	public String getCell(Cell thisCell) {
		if(thisCell != null && thisCell.getCellType() != Cell.CELL_TYPE_BLANK)
		{
			if(thisCell.getCellType() == Cell.CELL_TYPE_STRING) {
				return thisCell.getStringCellValue();
			}
			else if(thisCell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
				return thisCell.getNumericCellValue() + "";
			}
		}
		return "";
	}


}
