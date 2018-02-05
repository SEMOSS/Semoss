package prerna.ds.util;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.poi.main.helper.XLFileHelper;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ArrayUtilityMethods;

public class ExcelFileIterator extends AbstractFileIterator {

	private XLFileHelper helper;
	private ExcelQueryStruct qs;
	private int[] headerIndices;
	private String sheetToLoad;
	
	public ExcelFileIterator(ExcelQueryStruct qs) {
		this.qs = qs;
		String fileLocation = qs.getExcelFilePath();
		String sheetToLoad = qs.getSheetName();
		Map<String, String>  dataTypesMap = qs.getColumnTypes();
		Map<String, String> newHeaders = qs.getNewHeaderNames();
		this.helper = new XLFileHelper();
		this.helper.parse(fileLocation);	
		this.sheetToLoad = sheetToLoad;
		this.dataTypeMap = dataTypesMap;
		if(newHeaders != null && !newHeaders.isEmpty()) {
			this.newHeaders = newHeaders;
			Map<String, Map<String, String>> excelHeaderNames = new Hashtable<String, Map<String, String>>();
			excelHeaderNames.put(this.sheetToLoad, this.newHeaders);
			this.helper.modifyCleanedHeaders(excelHeaderNames);
		}
		
		setSelectors(qs.getSelectors());
		setFilters(qs.getFilters());
		
		if(dataTypesMap != null && !dataTypesMap.isEmpty()) {
			this.types = new String[this.headers.length];
			for(int j = 0; j < this.headers.length; j++) {
				this.types[j] = dataTypesMap.get(this.headers[j]);
			}
		}
		else {
			setUnknownTypes();
		}
		
		// need to grab the first row upon initialization 
		getNextRow();
	}
	
	private void setUnknownTypes() {
		this.types = this.helper.predictRowTypes(this.sheetToLoad, this.headerIndices);
		int numHeaders = this.headers.length;

		this.dataTypeMap = new Hashtable<String, String>();
		for(int i = 0; i < numHeaders; i++) {
			this.dataTypeMap.put(this.headers[i], types[i]);
		}
	}
	
	
	public void getNextRow() {
		String[] row = this.helper.getNextRow(this.sheetToLoad, this.headerIndices);
		
		if(filters == null || filters.isEmpty()) {
			this.nextRow = row;
			return;
		}
		
		String[] newRow = null;
		while (newRow == null && (row != null)) {

			Set<String> allFilteredCols = this.filters.getAllFilteredColumns();
			//isValid checks if the row meets all of the given filters 
			boolean isValid = true;
			for (String col : allFilteredCols) {
				int rowIndex = Arrays.asList(headers).indexOf(col);
				// check valid index
				if (rowIndex >= 0) {
					//list of all filters on a given column
					List<SimpleQueryFilter> nextSet = this.filters.getAllSimpleQueryFiltersContainingColumn(col);
					for (SimpleQueryFilter filter : nextSet) {
						//get all filter information
						FILTER_TYPE filterType = filter.getFilterType();
						NounMetadata leftComp = filter.getLComparison();
						NounMetadata rightComp = filter.getRComparison();
						String comparator = filter.getComparator();

						if (filterType == FILTER_TYPE.COL_TO_COL) {
							//TODO
							//isValid = isValid && filterColToCol(leftComp, rightComp, row, comparator, rowIndex);
						} else if (filterType == FILTER_TYPE.COL_TO_VALUES) {
							// Genre = ['Action'] example
							isValid = isValid && filterColToValues(leftComp, rightComp, row, comparator, rowIndex);

						} else if (filterType == FILTER_TYPE.VALUES_TO_COL) {
							// here the left and rightcomps are reversed, so send them to the method in opposite order and reverse comparator
							// 50000 > MovieBudget gets sent as MovieBudget < 50000
							isValid = isValid && filterColToValues(rightComp, leftComp, row, IQueryFilter.getReverseNumericalComparator(comparator), rowIndex);
						} else if (filterType == FILTER_TYPE.VALUE_TO_VALUE) {
							//?????????
							
						}

					}

				}

			}

			if (isValid) {
				newRow = row;
				break;
			} else {
				newRow = null;
			}

			if (newRow == null) {
				row = this.helper.getNextRow(this.sheetToLoad, this.headerIndices);
			}

		}
		this.nextRow = newRow;

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
	public void resetHelper() {
		this.helper.reset();
	}

	@Override
	public void clearHelper() {
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
