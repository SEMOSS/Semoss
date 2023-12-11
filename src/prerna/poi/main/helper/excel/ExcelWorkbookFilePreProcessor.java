package prerna.poi.main.helper.excel;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import prerna.util.Utility;

public class ExcelWorkbookFilePreProcessor {

	private	Workbook workbook = null;
	private FileInputStream sourceFile = null;
	private String fileLocation = null;
	
	private Map<String, ExcelSheetPreProcessor> sheetProcessor = null;
	
	public void parse(String fileLocation) {
		this.fileLocation = fileLocation;
		createParser();
	}
	
	/**
	 * Opens the workbook
	 */
	private void createParser() {
		try {
			sourceFile = new FileInputStream(Utility.normalizePath(fileLocation));
			try {
				workbook = WorkbookFactory.create(sourceFile);
			} catch (EncryptedDocumentException e) {
				e.printStackTrace();
			}
			sheetProcessor = new HashMap<String, ExcelSheetPreProcessor>();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Loop through all the sheets to determine the ranges of tables
	 */
	public void determineTableRanges() {
		int numSheets = workbook.getNumberOfSheets();
		for(int sheetIndex = 0; sheetIndex < numSheets; sheetIndex++) {
			determineTableRanges(workbook.getSheetAt(sheetIndex));
		}
	}
	
	/**
	 * Determine ranges in a specific sheet
	 * @param sheet
	 */
	private void determineTableRanges(Sheet sheet) {
		ExcelSheetPreProcessor sProcessor = new ExcelSheetPreProcessor(sheet);
		sProcessor.determineSheetRanges();
		sheetProcessor.put(sheet.getSheetName(), sProcessor);
	}
	
	public Map<String, ExcelSheetPreProcessor> getSheetProcessors() {
		if(this.sheetProcessor == null) {
			throw new IllegalArgumentException("Must run determineTableRanges method to initialize pre processing of excel file");
		}
		return this.sheetProcessor;
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
	
	/**
	 * Get the sheets in order
	 * @return
	 */
	public List<String> getSheetNames() {
		int numSheets = this.workbook.getNumberOfSheets();
		List<String> sheets = new ArrayList<String>(numSheets);
		for(int i = 0; i < numSheets; i++) {
			sheets.add(this.workbook.getSheetName(i));
		}
		return sheets;
	}
	
	
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////

//	public static void main(String[] args) {
//		String fileLocation = "C:\\Users\\SEMOSS\\Desktop\\shifted.xlsx";
//
//		ExcelWorkbookFilePreProcessor processor = new ExcelWorkbookFilePreProcessor();
//		processor.parse(fileLocation);
//		processor.determineTableRanges();
//		Map<String, ExcelSheetPreProcessor> sheetProcessors = processor.getSheetProcessors();
//		for(String sheet : sheetProcessors.keySet()) {
//			ExcelSheetPreProcessor sProcessor = sheetProcessors.get(sheet);
//			
//			{
//				List<ExcelBlock> blocks = sProcessor.getAllBlocks();
//				System.out.println("Streaming approach for types");
//				for(int i = 0; i < blocks.size(); i++) {
//					ExcelBlock block = blocks.get(i);
//					List<ExcelRange> blockRanges = block.getRanges();
//					for(int j = 0; j < blockRanges.size(); j++) {
//						ExcelRange r = blockRanges.get(j);
//						System.out.println("Found range = " + r.getRangeSyntax());
//						
//						System.out.println("Predicted range with headers " + Arrays.toString(sProcessor.getRangeHeaders(r)));
//						System.out.println("Predicted types for range");
//						Object[][] rangeTypes = block.getRangeTypes(r);
//						for(Object[] p : rangeTypes) {
//							System.out.println(Arrays.toString(p));
//						}
//					}
//				}
//			}
//			
//			System.out.println();
//			System.out.println();
//
//			{
//				System.out.println("Brute force method for types");
//				List<ExcelBlock> blocks = sProcessor.getAllBlocks();
//				for(int i = 0; i < blocks.size(); i++) {
//					ExcelBlock block = blocks.get(i);
//					List<ExcelRange> blockRanges = block.getRanges();
//					for(int j = 0; j < blockRanges.size(); j++) {
//						ExcelRange r = blockRanges.get(j);
//						System.out.println("Getting prediciton for range = " + r.getRangeSyntax());
//						Object[][] prediction = ExcelParsing.predictTypes(sProcessor.getSheet(), r.getRangeSyntax());
//						for(Object[] p : prediction) {
//							System.out.println(Arrays.toString(p));
//						}
//					}
//				}
//			}
//		}
//	}
}
