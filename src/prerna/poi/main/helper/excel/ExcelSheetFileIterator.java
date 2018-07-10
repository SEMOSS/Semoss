package prerna.poi.main.helper.excel;

import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.util.flatfile.AbstractFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class ExcelSheetFileIterator extends AbstractFileIterator {

	// classes around the sheet
	private Sheet sheet;
	private ExcelSheetPreProcessor sProcessor;
	private ExcelRange range;
	private int[] rangeIndex;
	
	// classes around the query struct
	private ExcelQueryStruct qs;
	private String sheetRange;
	
	// speed improvements
	private String[] headers;
	private int[] headerIndices;
	private int numHeaders;
	
	// for looping through 
	private int curRow;
	private int startCol;
	private int endRow;
	
	public ExcelSheetFileIterator(Sheet sheet, ExcelQueryStruct qs) {
		// get the excel elements
		this.sheet = sheet;
		this.sProcessor = new ExcelSheetPreProcessor(this.sheet);
		
		// get the qs elements
		this.qs = qs;
		this.sheetRange = qs.getSheetRange();
		
		// range index is start col, start row, end col, end row
		this.range = new ExcelRange(this.sheetRange);
		this.rangeIndex = range.getIndices();
		
		// this will be the first row of data
		// since excel is 1 based and java is 0
		this.curRow = this.rangeIndex[1];
		this.startCol = this.rangeIndex[0];
		this.endRow = this.rangeIndex[3];
		
		// now that I have set the headers from the setSelectors
		this.dataTypeMap = qs.getColumnTypes();
		this.additionalTypesMap = qs.getAdditionalTypes();
		this.newHeaders = qs.getNewHeaderNames();
		
		// need to figure out the selectors
		setSelectors(qs.getSelectors());
		
		if(this.dataTypeMap != null && !this.dataTypeMap.isEmpty()) {
			this.types = new SemossDataType[this.headers.length];
			this.additionalTypes = new String[this.headers.length];
			
			for (int index = 0; index < this.headers.length; index++) {
				this.types[index] = SemossDataType.convertStringToDataType(dataTypeMap.get(this.headers[index]));
				if(this.additionalTypesMap != null) {
					this.additionalTypes[index] = additionalTypesMap.get(this.headers[index]);
				}
			}
		}
		else {
			setUnknownTypes();
			qs.setColumnTypes(this.dataTypeMap);
			qs.setAdditionalTypes(this.additionalTypesMap);
		}
		
		this.numHeaders = this.headerIndices.length;
		// grab the first row in preparation for iterating
		getNextRow();
	}
	
	
	@Override
	public void getNextRow() {
		if(this.curRow >= this.endRow) {
			this.nextRow = null;
			return;
		}
		
		// get the new row to return
		this.nextRow = new Object[this.headerIndices.length];

		Row row = this.sheet.getRow(this.curRow);
		if(row != null) {
			for(int i = 0; i < numHeaders; i++) {
				int cellIndex = this.headerIndices[i];
				// remember, excel is 1 based while java is 0
				Cell c = row.getCell(cellIndex-1);
				this.nextRow[i] = ExcelParsing.getCell(c);
			}
		} else {
			// set all values to empty string
			for(int i = 0; i < this.headerIndices.length; i++) {
				this.nextRow[i] = "";
			}
		}
		// set up for the next row
		this.curRow++;
	}
	
	
	@Override
	public IHeadersDataRow next() {
		Object[] row = nextRow;
		getNextRow();

		// couple of things to take care of here
		Object[] cleanRow = cleanRow(row, types, additionalTypes);
		IHeadersDataRow nextData = new HeadersDataRow(this.headers, cleanRow, cleanRow);
		return nextData;
	}
	
	/**
	 * Since we have types in excel
	 * We will use a better version for getting the clean types
	 */
	@Override
	protected Object[] cleanRow(Object[] row, SemossDataType[] types, String[] additionalTypes) {
		Object[] cleanRow = new Object[row.length];
		for(int i = 0; i < row.length; i++) {
			Object val = row[i];
			SemossDataType type = types[i];
			String additionalFormatting = additionalTypes[i];
			
			// try to get correct type
			if(type == SemossDataType.STRING) {
				cleanRow[i] = Utility.cleanString(val.toString(), true, true, false);
			} else if(type == SemossDataType.INT) {
				if(val instanceof Number) {
					cleanRow[i] = ((Number) val).intValue();
				} else {
					String strVal = val.toString();
					try {
						//added to remove $ and , in data and then try parsing as Double
						int mult = 1;
						if(strVal.startsWith("(") || strVal.startsWith("-")) { // this is a negativenumber
							mult = -1;
						}
						strVal = strVal.replaceAll("[^0-9\\.E]", "");
						cleanRow[i] = mult * Integer.parseInt(strVal.trim());
					} catch(NumberFormatException ex) {
						//do nothing
						cleanRow[i] = null;
					}
				}
			} else if(type == SemossDataType.DOUBLE) {
				if(val instanceof Number) {
					cleanRow[i] = ((Number) val).doubleValue();
				} else {
					String strVal = val.toString();
					try {
						//added to remove $ and , in data and then try parsing as Double
						int mult = 1;
						if(strVal.startsWith("(") || strVal.startsWith("-")) { // this is a negativenumber
							mult = -1;
						}
						strVal = strVal.replaceAll("[^0-9\\.E]", "");
						cleanRow[i] = mult * Double.parseDouble(strVal.trim());
					} catch(NumberFormatException ex) {
						//do nothing
						cleanRow[i] = null;
					}
				}
			} else if(type == SemossDataType.DATE) {
				if(val instanceof SemossDate) {
					if(additionalFormatting != null) {
						cleanRow[i] = new SemossDate(((SemossDate) val).getDate(), additionalFormatting);
					} else {
						cleanRow[i] = val;
					}
				} else {
					String strVal = val.toString();
					if(additionalFormatting != null) {
						cleanRow[i] = new SemossDate(strVal, additionalFormatting);
					} else {
						cleanRow[i] = SemossDate.genDateObj(strVal);
					}
				}
			} else if(type == SemossDataType.TIMESTAMP) {
				if(val instanceof SemossDate) {
					if(additionalFormatting != null) {
						cleanRow[i] = new SemossDate(((SemossDate) val).getDate(), additionalFormatting);
					} else {
						cleanRow[i] = val;
					}
				} else {
					String strVal = val.toString();
					if(additionalFormatting != null) {
						cleanRow[i] = new SemossDate(strVal, additionalFormatting);
					} else {
						cleanRow[i] = SemossDate.genTimeStampDateObj(strVal);
					}
				}
			}
		}
		
		return cleanRow;
	}
	
	/**
	 * Determine the selectors for the sheet
	 * @param qsSelectors
	 */
	private void setSelectors(List<IQuerySelector> qsSelectors) {
		if(qsSelectors.isEmpty()) {
			 // if no selectors, return everything
			this.headers = this.sProcessor.getCleanedRangeHeaders(this.range);
			this.headerIndices = new int[this.headers.length];
			for(int i = 0; i < this.headers.length; i++) {
				this.headerIndices[i] = i + startCol;
				if(this.newHeaders.containsKey(this.headers[i])) {
					this.headers[i] = this.newHeaders.get(this.headers[i]);
				}
			}
			return;
		}
		int numSelectors = qsSelectors.size();

		String[] selectors = new String[numSelectors];
		for(int i = 0; i < numSelectors; i++) {
			QueryColumnSelector newSelector = (QueryColumnSelector) qsSelectors.get(i);
			if(newSelector.getSelectorType() != IQuerySelector.SELECTOR_TYPE.COLUMN) {
				throw new IllegalArgumentException("Cannot perform math on a excel import");
			}
			selectors[i] = newSelector.getAlias();
		}
		
		String[] allHeaders = this.sProcessor.getCleanedRangeHeaders(this.range);
		if(allHeaders.length != selectors.length) {
			// order the selectors
			// all headers will be ordered
			String[] orderedSelectors = new String[selectors.length];
			int counter = 0;
			for(String header : allHeaders) {
				if(ArrayUtilityMethods.arrayContainsValue(selectors, header)) {
					orderedSelectors[counter] = header;
					counter++;
				}
			}
			
			this.headers = orderedSelectors;
			this.headerIndices = getHeaderIndicies(allHeaders, orderedSelectors);
			for(int i = 0; i < this.headers.length; i++) {
				if(this.newHeaders.containsKey(this.headers[i])) {
					this.headers[i] = this.newHeaders.get(this.headers[i]);
				}
			}
		} else {
			this.headers = allHeaders;
			this.headerIndices = new int[this.headers.length];
			for(int i = 0; i < this.headers.length; i++) {
				this.headerIndices[i] = i + startCol;
				if(this.newHeaders.containsKey(this.headers[i])) {
					this.headers[i] = this.newHeaders.get(this.headers[i]);
				}
			}
		}
	}
	
	/**
	 * Sets the data types 
	 */
	private void setUnknownTypes() {
		Object[][] prediction = ExcelParsing.predictTypes(this.sheet, this.sheetRange);
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(this.headers, prediction);
		this.dataTypeMap = predictionMaps[0];
		this.additionalTypesMap = predictionMaps[1];
		
		// need to redo types to be only those in the selectors
		this.types = new SemossDataType[this.headers.length];
		this.additionalTypes = new String[this.headers.length];
		for(int i = 0; i < this.headers.length; i++) {
			this.types[i] = SemossDataType.convertStringToDataType(this.dataTypeMap.get(this.headers[i]));
			this.additionalTypes[i] = this.additionalTypesMap.get(this.headers[i]);
		}
	}
	
	/**
	 * Get the indicies for the headers within the excel block
	 * @param sheetHeaders
	 * @param headers
	 * @return
	 */
	private int[] getHeaderIndicies(String[] sheetHeaders, String[] headers) {
		int numHeadersToGet = headers.length;
		int[] indicesToGet = new int[numHeadersToGet];
		for(int colIdx = 0; colIdx < numHeadersToGet; colIdx++) {
			String headerToGet = headers[colIdx];
			// find the index in sheet headers to return
			// add start col so the offset is accurate
			indicesToGet[colIdx] = ArrayUtilityMethods.arrayContainsValueAtIndex(sheetHeaders, headerToGet) + startCol;
		}
		
		return indicesToGet;
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
	}


	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub
	}
	
	public ExcelQueryStruct getQs() {
		return this.qs;
	}

	public void setQs(ExcelQueryStruct qs) {
		this.qs = qs;
	}
	
}