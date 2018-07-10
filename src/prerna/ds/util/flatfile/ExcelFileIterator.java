package prerna.ds.util.flatfile;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.poi.main.helper.XLFileHelper;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

@Deprecated
public class ExcelFileIterator extends AbstractFileIterator {

	private XLFileHelper helper;
	private ExcelQueryStruct qs;
	private int[] headerIndices;
	private String sheetToLoad;
	
	public ExcelFileIterator(ExcelQueryStruct qs) {
		this.qs = qs;
		this.fileLocation = qs.getFilePath();
		this.sheetToLoad = qs.getSheetName();
		
		this.dataTypeMap = qs.getColumnTypes();
		this.newHeaders = qs.getNewHeaderNames();
		
		this.helper = new XLFileHelper();
		this.helper.parse(this.fileLocation);
		
		if(newHeaders != null && !newHeaders.isEmpty()) {
			Map<String, Map<String, String>> excelHeaderNames = new Hashtable<String, Map<String, String>>();
			excelHeaderNames.put(this.sheetToLoad, this.newHeaders);
			this.helper.modifyCleanedHeaders(excelHeaderNames);
		}
		setSelectors(qs.getSelectors());
		
		// now that I have set the headers from the setSelectors
		this.headers = this.helper.getHeaders(this.sheetToLoad);
		this.additionalTypesMap = qs.getAdditionalTypes();
		
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
			setSelectors(qs.getSelectors());
			qs.setColumnTypes(this.dataTypeMap);
		}
		
		// need to grab the first row upon initialization 
		getNextRow();
	}
	
	private void setUnknownTypes() {
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(this.sheetToLoad), helper.predictTypes(this.sheetToLoad, this.headerIndices));
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
	
	@Override
	public IHeadersDataRow next() {
		Object[] row = nextRow;
		getNextRow();

		// couple of things to take care of here
		Object[] cleanRow = cleanRow(row, types, additionalTypes);
		IHeadersDataRow nextData = new HeadersDataRow(this.headers, cleanRow, cleanRow);
		return nextData;
	}
	
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
	
	@Override
	public void getNextRow() {
		Object[] row = this.helper.getNextRow(this.sheetToLoad, this.headerIndices);
		this.nextRow = row;
	}

	private void setSelectors(List<IQuerySelector> qsSelectors) {
		if(qsSelectors.isEmpty()) {
			return; // if no selectors, return everything
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
		
		String[] allHeaders = this.helper.getHeaders(this.sheetToLoad);
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
			this.headerIndices = this.helper.getHeaderIndicies(this.sheetToLoad, orderedSelectors);
			this.helper.getNextRow(this.sheetToLoad, this.headerIndices); // after redoing the selectors, we need to skip the headers 
		} else {
			this.headers = allHeaders;
			this.headerIndices = new int[this.headers.length];
			for(int i = 0; i < this.headers.length; i++) {
				this.headerIndices[i] = i;
			}
		}
	}

	@Override
	public void reset() {
		this.helper.reset();
	}

	@Override
	public void cleanUp() {
		this.helper.clear();
	}
	
	public XLFileHelper getHelper() {
		return this.helper;
	}
	
	public ExcelQueryStruct getQs() {
		return this.qs;
	}

	public void setQs(ExcelQueryStruct qs) {
		this.qs = qs;
	}
}
