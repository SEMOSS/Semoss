package prerna.poi.main.helper.excel;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import prerna.query.querystruct.ExcelQueryStruct;

public class ExcelWorkbookFileHelper {

	private	Workbook workbook = null;
	private FileInputStream sourceFile = null;
	private String fileLocation = null;
	
	public void parse(String fileLocation) {
		this.fileLocation = fileLocation;
		createParser();
	}
	
	/**
	 * Opens the workbook
	 */
	private void createParser() {
		try {
			sourceFile = new FileInputStream(fileLocation);
			try {
				workbook = WorkbookFactory.create(sourceFile);
			} catch (EncryptedDocumentException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Get all sheets
	 * @return
	 */
	public List<String> getSheets() {
		int numSheets = workbook.getNumberOfSheets();
		List<String> sheets = new Vector<String>();
		for(int i = 0; i < numSheets; i++) {
			sheets.add(workbook.getSheetName(i));
		}
		
		return sheets;
	}
	
	/**
	 * Get the Sheet object
	 * @param sheetName
	 * @return
	 */
	public Sheet getSheet(String sheetName) {
		return workbook.getSheet(sheetName);
	}
	
	/**
	 * Get the file path 
	 */
	public String getFilePath() {
		return fileLocation;
	}
	
	/**
	 * 
	 * @param sheetName
	 * @param excelRange
	 * @param dataTypes
	 * @param additionalTypes
	 */
	public ExcelSheetFileIterator getSheetIterator(ExcelQueryStruct qs) {
		String sheetName = qs.getSheetName();
		Sheet sheet = workbook.getSheet(sheetName);
		ExcelSheetFileIterator it = new ExcelSheetFileIterator(sheet, qs);
		return it;
	}
	
	/**
	 * Builder to get to the sheet iterator
	 * @param qs
	 * @return
	 */
	public static ExcelSheetFileIterator buildSheetIterator(ExcelQueryStruct qs) {
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(qs.getFilePath());
		return helper.getSheetIterator(qs);
	}
	
	/**
	 * Clears the parser and requires you to start the parsing from scratch	
	 */
	public void clear() {
		try {
			if(sourceFile != null) {
				sourceFile.close(); 
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////

//	public static void main(String[] args) {
//		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
//		
//		String fileLocation = "C:\\Users\\SEMOSS\\Desktop\\shifted.xlsx";
//		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
//		helper.parse(fileLocation);
//		System.out.println(helper.getSheets());
//		
//		
//		ExcelQueryStruct qs = new ExcelQueryStruct();
//		qs.setSheetName("Sheet1");
//		qs.setSheetRange("E7:R28");
//		
//		ExcelSheetFileIterator it = helper.getSheetIterator(qs);
//		while(it.hasNext()) {
//			System.out.println(Arrays.toString(it.next().getValues()));
//		}
//	}
//	
}
