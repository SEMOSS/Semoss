package prerna.poi.main.helper.excel;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.poi.main.HeadersException;

public class ExcelSheetPreProcessor {

	private static final Logger LOGGER = LogManager.getLogger(ExcelSheetPreProcessor.class.getName());

	private Sheet sheet;
	private String sheetName;
	
	// contain a list of all the blocks within this sheet
	private List<ExcelBlock> allBlocks = new Vector<ExcelBlock>();
	
	public ExcelSheetPreProcessor(Sheet sheet) {
		this.sheet = sheet;
		this.sheetName = sheet.getSheetName();
	}
	
	public Sheet getSheet() {
		return this.sheet;
	}
	
	public List<ExcelBlock> getAllBlocks() {
		return this.allBlocks;
	}
	
	public String[] getRangeHeaders(ExcelRange range) {
		int[] rangeIndices = range.getIndices();
		// need to get the first row
		
		int startCol = rangeIndices[0]-1;
		int startRow = rangeIndices[1]-1;
		int endCol = rangeIndices[2];

		Row headerRow = sheet.getRow(startRow);
		String[] curHeaders = new String[endCol - startCol];
		
		int counter = 0;
		for(int i = startCol; i < endCol; i++) {
			curHeaders[counter] = ExcelParsing.getCell(headerRow.getCell(i)) + "";
			counter++;
		}
		
		return curHeaders;
	}
	
	public String[] getCleanedRangeHeaders(ExcelRange range) {
		String[] oHeaders = getRangeHeaders(range);
		
		// grab the headerChecker
		HeadersException headerChecker = HeadersException.getInstance();
		List<String> newUniqueCleanHeaders = new Vector<String>();
		
		int numCols = oHeaders.length;
		for(int colIdx = 0; colIdx < numCols; colIdx++) {
			String origHeader = oHeaders[colIdx];
			if(origHeader.trim().isEmpty()) {
				origHeader = "BLANK_HEADER";
			}
			String newHeader = headerChecker.recursivelyFixHeaders(origHeader, newUniqueCleanHeaders);
			
			// now update the unique headers, as this will be used to match duplications
			newUniqueCleanHeaders.add(newHeader);
		}
		
		return newUniqueCleanHeaders.toArray(new String[]{});
	}
	
	/**
	 * Determine table ranges within a specific sheet
	 */
	public void determineSheetRanges() {
		int startRow = sheet.getFirstRowNum();
		int lastRow = sheet.getLastRowNum();
		
		ExcelBlock thisBlock = new ExcelBlock();
		
		LOGGER.info("Processing " + sheetName + " from rows " + startRow + " to " + lastRow);
		for(int rowIndex = startRow; rowIndex <= lastRow; rowIndex++) {
			Row thisRow = sheet.getRow(rowIndex);
			
			// if i have a null row then we have a new block
			if(thisRow == null) {
				if(!thisBlock.isEmpty()) {
					// add to the list of blocks
					allBlocks.add(thisBlock);
					// create a new block
					thisBlock = new ExcelBlock();
				}
				// continue to the next row
				continue;
			}
			
			int startCol = thisRow.getFirstCellNum();
			int lastCol = thisRow.getLastCellNum();
			
			// sometimes, we can have an empty row
			// treat this as being a null row as well
			if(lastCol <= 0) {
				if(thisBlock.numIndicesInBlock() > 1) {
					// add to the list of blocks
					allBlocks.add(thisBlock);
					// create a new block
					thisBlock = new ExcelBlock();
				}
				// continue to the next row
				continue;
			}
			
			// we want to keep track
			// if we are at the first column
			boolean initStartCol = true;
			int filledInColumns = 0;
			
			// loop through the row and add to the current block
			for(int colIndex = startCol; colIndex <= lastCol; colIndex++) {
				Cell thisCell = thisRow.getCell(colIndex);
				Object cellValue = ExcelParsing.getCell(thisCell);
				// if the cell is empty
				if(cellValue.toString().isEmpty()) {
					// ignore
					continue;
				} else {
					if(initStartCol) {
						thisBlock.addStartColumnIndex(colIndex);
						initStartCol = false;
					}
					
					// add column to row + type metadata
					String additionalFormatting = null;
					if(cellValue instanceof SemossDate) {
						additionalFormatting = ((SemossDate) cellValue).getPattern();
					}
					
					SemossDataType cellType = ExcelParsing.getTypeByCast(cellValue);
					thisBlock.addColumnToRowIndexWithData(colIndex, rowIndex+1, cellType, additionalFormatting);
					filledInColumns++;
				}
			}
			
			// now see if the block is the same as the last one or not
			if(filledInColumns > 0) {
				// add the total number of columns that have values
				thisBlock.addTotalColumnsInRowStats(filledInColumns);
				// add the row index that has data
				thisBlock.addRowIndexContainingData(rowIndex);
				// set the max column
				thisBlock.trySetLastColMaxIndex(lastCol);
			} else if(!thisBlock.isEmpty()) {
				tryMergeBlocks(thisBlock);
				// create a new block
				thisBlock = new ExcelBlock();
			}
		}
		
		// we gotta add the last block into the list
		if(thisBlock.numIndicesInBlock() > 1) {
			this.allBlocks.add(thisBlock);
		}
	}
	
	private void tryMergeBlocks(ExcelBlock thisBlock) {
		if(!allBlocks.isEmpty()) {
			// see if this was the same as the last block
			// or add it to the list of blocks
			ExcelBlock lastBlock = allBlocks.get(allBlocks.size()-1);
			if(lastBlock.sameAs(thisBlock)) {
				lastBlock.merge(thisBlock);
			} else {
				allBlocks.add(thisBlock);
			}
		} else {
			// add our first block
			allBlocks.add(thisBlock);
		}
	}
	
}
