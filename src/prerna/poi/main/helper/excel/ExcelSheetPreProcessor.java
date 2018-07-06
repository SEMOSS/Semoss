package prerna.poi.main.helper.excel;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import prerna.date.SemossDate;

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
	
	public List<ExcelBlock> getAllBlocks() {
		return this.allBlocks;
	}
	
	public List<ExcelRange> getExcelRanges() {
		List<ExcelRange> allRanges = new Vector<ExcelRange>();
		for(ExcelBlock b : allBlocks) {
			List<ExcelRange> ranges = b.getRanges();
			allRanges.addAll(ranges);
		}
		
		for(ExcelRange r : allRanges) {
			LOGGER.info("Found range in " + sheetName + " = " + r.getRangeSyntax());
		}
		
		return allRanges;
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
			if(lastCol == 0) {
				if(!thisBlock.isEmpty()) {
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
				Object cellValue = getCell(thisCell);
				// if the cell is empty
				if(cellValue.toString().isEmpty()) {
					// ignore
					continue;
				} else {
					if(initStartCol) {
						thisBlock.addStartColumnIndex(colIndex);
						initStartCol = false;
					}
					thisBlock.addColumnToRowIndexWithData(colIndex, rowIndex);
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
		if(!thisBlock.isEmpty()) {
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
	
	/**
	 * Get the cell values
	 * @param thisCell
	 * @return
	 */
	private Object getCell(Cell thisCell) {
		if(thisCell == null) {
			return "";
		}
		int type = thisCell.getCellType();
		if(type == Cell.CELL_TYPE_BLANK) {
			return "";
		}
		if(type == Cell.CELL_TYPE_STRING) {
			return thisCell.getStringCellValue();
		} else if(type == Cell.CELL_TYPE_NUMERIC) {
			if(DateUtil.isCellDateFormatted(thisCell)) {
				return new SemossDate(thisCell.getDateCellValue(), thisCell.getCellStyle().getDataFormatString());
			}
			return thisCell.getNumericCellValue();
		} else if(type == Cell.CELL_TYPE_BOOLEAN) {
			return thisCell.getBooleanCellValue() + "";
		} else if(type == Cell.CELL_TYPE_FORMULA) {
			// do the same for the formula value
			int formulatype = thisCell.getCachedFormulaResultType();
			if(formulatype == Cell.CELL_TYPE_BLANK) {
				return "";
			}
			if(formulatype == Cell.CELL_TYPE_STRING) {
				return thisCell.getStringCellValue();
			} else if(formulatype == Cell.CELL_TYPE_NUMERIC) {
				if(DateUtil.isCellDateFormatted(thisCell)) {
					return new SemossDate(thisCell.getDateCellValue(), thisCell.getCellStyle().getDataFormatString());
				}
				return thisCell.getNumericCellValue();
			} else if(formulatype == Cell.CELL_TYPE_BOOLEAN) {
				return thisCell.getBooleanCellValue();
			}
		}
		return "";
	}
	
}
