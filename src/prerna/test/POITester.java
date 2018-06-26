package prerna.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellRangeAddress;

public class POITester {
	/*
	 * Things to take care of
	 * a. I need to run through atleast 100 rows or so to predict where the row beginning is
	 * b. If the first row is merged - then it is a title and not a header and needs to be ignored
	 * c. If the first row is all empty - then it is just a space
	 * d. If the first row has a few values only - I need to find what is the most number of columns and then see which is the first row which has the same number of columns
	 * e. When I run through rows - if I see 2 or more consecutive rows have no data, that indicates I need to stop - unless the user has ignored line breaks
	 * f. Once I have the headers and I see a merged area - Need to do other magic like the web scraper
	 * 
	 * also need to see what are the other blocks 
	 * 
	 * 
	 * 
	 * 
	 */
	
	Hashtable rowRegions = new Hashtable();
	SummaryStatistics filledCols = new SummaryStatistics();
	SummaryStatistics startCols = new SummaryStatistics();
	SummaryStatistics endCols = new SummaryStatistics();
	

	List <Integer> rows = new Vector<Integer>();
	List <Integer> filledColsData = new Vector<Integer>();
	List <Integer> startColsData = new Vector<Integer>();
	
	POIBlock curBlock = null;
	
	List <POIBlock> allBlocks = new Vector<POIBlock>();
	
	public static void main(String [] args)
	{
		POITester tester = new POITester();
		//tester.processFile("c:\\users\\pkapaleeswaran\\workspacej3\\datasets\\Tax.xlsx");
		//tester.processFile("c:\\users\\pkapaleeswaran\\workspacej3\\datasets\\SEMOSS Sprint Tasks.xlsx");
		//tester.processFile("c:\\users\\pkapaleeswaran\\workspacej3\\datasets\\RMF.xlsx");
		tester.processFile("c:\\users\\pkapaleeswaran\\workspacej3\\datasets\\ExcelTest.xlsx");
		
	}
	
	// get information as x and y
	public void getXY()
	{
		
	}
	
	
	
	
	
	public void processFile(String file)
	{
		try {
			Workbook workbook = WorkbookFactory.create(new FileInputStream(file));
			for(int sheetIndex = 0;sheetIndex < workbook.getNumberOfSheets();sheetIndex++)
				predictHeader(workbook.getSheetAt(sheetIndex));
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
	
	public Vector<Range> predictHeader(String fileName, String sheetName)
	{
		try {
			Workbook workbook = WorkbookFactory.create(new FileInputStream(fileName));
			return predictHeader(workbook.getSheet(sheetName));
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
		return null;
		
	}

	// predict which row is the header
	// I need atleast around 100 rows 
	// so I can actually see what is the best row
	public Vector<Range> predictHeader(Sheet sheet)
	{
		//makeMergeMap(sheet);
		// reset everything
		resetAll();
		
		System.out.println("Processing.. " + sheet.getSheetName());
		
		// get the number of columns
		getNumCols(sheet);
		
		Vector <Range> allRanges = new Vector<Range>();
		
		//System.out.println("Total Blocks.. " + allBlocks.size());
		
		for(int blockIndex = 0;blockIndex < allBlocks.size();blockIndex++)
		{
			//System.out.println("Printing Block..  " + blockIndex);
			POIBlock block = allBlocks.get(blockIndex);
			allRanges.addAll(block.getRange());
			block.print();
		}
		System.out.println("---------------------");
		
		/*
		POIBlock aBlock = allBlocks.get(0);
		// ok now figure out what is the number of filled columns average
		Double mean = aBlock.filledCols.getMean();
		int numCols = mean.intValue();
		
		// startCols
		mean = aBlock.startCols.getMean();
		
		int startRow = 0;
		// now get to the start row
		// this is basically the first row which has numcols >= mean
		for(int filledIndex = 0;filledIndex < aBlock.filledColsData.size();filledIndex++)
		{
			if(aBlock.filledColsData.get(filledIndex) >= numCols)
			{
				startRow = aBlock.rows.get(filledIndex);
				break;
			}
		}
		
		int startCol = new Double(aBlock.startCols.getMean()).intValue();
		//System.out.println("Number of blocks " + allBlocks.size());
		
		// and we have the winner
		/*System.out.println("Starting Row.. " +  startRow);
		System.out.println("Starting cols >> " + startCol);
		System.out.println("Number of cols to pull >> " + (aBlock.endCols.getMax() - aBlock.startCol));
		System.out.println("Deviation..  " + aBlock.filledCols.getStandardDeviation());
		System.out.println("Last Row.. " + aBlock.rows.get(aBlock.rows.size() - 1));
		*/
		
		
		//Row row = sheet.getRow(aBlock.rows.get(aBlock.rows.size() - 1));
		
		
/*		for(int colIndex = mean.intValue();colIndex <= numCols;colIndex++)
		{
			System.out.print(colIndex + ":" + getCell(row.getCell(colIndex)));
		}
		System.out.println("");
*/		
		return allRanges;
	}
	
	public void resetAll()
	{
		filledCols = new SummaryStatistics();
		startCols = new SummaryStatistics();
		rows = new Vector<Integer>();
		
		filledColsData = new Vector<Integer>();
		startColsData = new Vector<Integer>();
		
		
		allBlocks = new Vector<POIBlock>();
		curBlock = null;

	}
	
	public void getNumCols(Sheet sheet)
	{
		// defaulting to 100
		// I am trying to determine 
		//where exactly is the column starting
		// how many columns are there - what is the last column
		// row_NUMCOLS - number of columns
		// row_<COLNUMBER> - which columns are empty
		// row_FNUM_COLS - how many columns are filled
		
		Hashtable rowToNCols = new Hashtable();
		
		if(curBlock == null)
			curBlock = new POIBlock();

		
		boolean rowAdded = false;
		System.out.println("Last Row Num " + sheet.getLastRowNum());
		
		for(int rowIndex = 0;rowIndex <= sheet.getLastRowNum();rowIndex++)
		{
			Row thisRow = sheet.getRow(rowIndex);
			if(thisRow != null) // if it is merged move to next one
			{
				int startCell = thisRow.getFirstCellNum();
				//System.out.println("First cell is .. " + startCell);
				int lastCell = thisRow.getLastCellNum();
				
				boolean startColComplete = false;
				int numFilledCols = 0;
				int totalCols = 0;
				
				//rowToCols.put(rowIndex +"_NUMCOLS", value)
				for(int colIndex = startCell;lastCell > 0 && colIndex <= lastCell;colIndex++)
				{
					//System.out.println("col.. "+ colIndex);
					Cell cell = thisRow.getCell(colIndex);
					String value = getCell(cell);
					if(value != null && value.length() != 0)
					{
						numFilledCols = numFilledCols + 1;
						if(!startColComplete)
						{
							rowAdded = true;
							curBlock.startCols.addValue(colIndex);
							curBlock.startColsData.add(colIndex);
							startColComplete = true;
						}
						totalCols = colIndex;
						curBlock.addRowForCol(colIndex, rowIndex);
					}
					else // needs to be at the end of it
					{
						// if there is already a header.. ignore this
						// if the header is not there.. then this is a new block
						// need to create a new block
						// reset the row
						// columns, numfilled columns etc. 
						//System.out.println("There should be a new block here at >> " + colIndex);
						curBlock.addEmptyCol(colIndex);
					}
				}
				// if the entire row is blank dont even bother
				if(numFilledCols > 0)
				{
					curBlock.filledCols.addValue(numFilledCols);
					curBlock.filledColsData.add(numFilledCols);
					curBlock.endCols.addValue(totalCols);
					curBlock.endColsData.addValue(lastCell);
					curBlock.rows.add(rowIndex);
				}	
				else if(curBlock.rows.size() > 0)
				{
					//System.out.println("Processing..  " + rowIndex);
					// somehow I need to see if this is a new block
					// try to see if I can merge it here
					curBlock = mergeBlock(curBlock);
					rowAdded = false;
					startColComplete = false;
				}
			} // this is if the rows are not getting filled.. if the rows are completely null then this is a new block
			else if(curBlock.rows.size() > 0)
			{
				curBlock = mergeBlock(curBlock);
				rowAdded = false;
			}
		}
		if(curBlock.rows.size() > 0)
			mergeBlock(curBlock);
	}
	
	public POIBlock mergeBlock(POIBlock thisBlock)
	{
		POIBlock retBlock = null;
		if(allBlocks.size() > 0)
		{
			POIBlock lastBlock = allBlocks.get(allBlocks.size() - 1);
			if(lastBlock.sameAs(thisBlock))
			{
				lastBlock.mergeBlock(thisBlock);
				//System.out.println("Merged.. ");
				retBlock = new POIBlock();
			}
			else
			{
				allBlocks.add(thisBlock);
				retBlock = new POIBlock();
			}	
		}
		else
		{
			allBlocks.add(thisBlock);
			retBlock = new POIBlock();
		}
		return retBlock;
	}
	
	public void checkBlocks()
	{
		// For every block
		// see what hte start column and end column is
		// and see if there is a column that is consistently blank
		// if so these are 2 separate tables
		
		
		
		
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

	
	public void getHeaderRow(Sheet sheet)
	{
		Hashtable sheetNameToRow = new Hashtable();
		
	}
	
	public boolean isRowMerged(int rowNum)
	{
		return rowRegions.containsKey(rowNum);
	}
	
	public void makeMergeMap(Sheet sheet)
	{
		if(rowRegions == null)
		{
			rowRegions = new Hashtable();
			
			List <CellRangeAddress> regions = sheet.getMergedRegions();
			for(int regionIndex = 0;regionIndex < regions.size();regionIndex++)
			{
				CellRangeAddress thisAddress = regions.get(regionIndex);
				int firstRow = thisAddress.getFirstRow();
				int lastRow = thisAddress.getLastRow();
				
				int [] cols = new int[2];
				
				cols[0] = thisAddress.getFirstColumn();
				cols[1] = thisAddress.getLastColumn();
				
				for(int rowIndex = firstRow;rowIndex <= lastRow;rowIndex++)
				{
					rowRegions.put(rowIndex, cols);
					rowRegions.put(rowIndex, cols);
				}
			}
		}
	}
	
	
}
